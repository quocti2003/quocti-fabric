# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "3748fa11-a97f-43a4-9d4a-2fb72c9cd4af",
# META       "default_lakehouse_name": "lh_wwi",
# META       "default_lakehouse_workspace_id": "95db64f3-a8cc-49fc-8018-4624f6a69eca",
# META       "known_lakehouses": [
# META         {
# META           "id": "3748fa11-a97f-43a4-9d4a-2fb72c9cd4af"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# This is an INCREMENTAL load — read all batches newer than marker
# and dedup latest version per business_key (Bronze can hold
# multiple versions of the same InvoiceID over many days).

# Imports
from pyspark.sql.functions import (
    col, lit, sha2, concat_ws, coalesce, trim,
    max as spark_max,
    row_number, desc
)
from pyspark.sql.window import Window
from datetime import datetime


# Consts for Invoices flow
BRONZE_PATH    = "Files/bronze/wwi_invoices/"
SILVER_TABLE   = "silver.wwi_invoices"
BUSINESS_KEY   = "InvoiceID"
WATERMARK_FLOW = "bronze_wwi_invoices_TO_silver_wwi_invoices"

# DEDUP_ORDER_COL — source column used for incremental dedup.
# Same InvoiceID can appear in Bronze across multiple days because
# OLTP edits the row repeatedly. ORDER BY LastEditedWhen DESC keeps
# the most recently edited version per InvoiceID.
DEDUP_ORDER_COL = "LastEditedWhen"

# Business columns — used to compute row_hash + select on INSERT.
# Does NOT include: BUSINESS_KEY (handled separately) + audit cols.
BUSINESS_COLS = [
    "CustomerID", "BillToCustomerID", "OrderID", "DeliveryMethodID",
    "ContactPersonID", "AccountsPersonID", "SalespersonPersonID", "PackedByPersonID",
    "InvoiceDate", "CustomerPurchaseOrderNumber", "IsCreditNote", "CreditNoteReason",
    "Comments", "DeliveryInstructions", "InternalComments",
    "TotalDryItems", "TotalChillerItems", "DeliveryRun", "RunPosition",
    "ReturnedDeliveryData", "ConfirmedDeliveryTime", "ConfirmedReceivedBy",
    "LastEditedBy", "LastEditedWhen"
]

# Full Silver column list in DDL order (key + business + meta).
# Used in .select(*ALL_SILVER_COLS) to enforce column order on append.
ALL_SILVER_COLS = [BUSINESS_KEY] + BUSINESS_COLS + [
    "audit_ts",
    "deleted_audit_ts",
    "source_id",
    "row_hash"
]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_df = spark.read.table(SILVER_TABLE)

max_audit_row = silver_df.agg(
    spark_max("audit_ts").alias("max_silver_audit_ts")
).first()

# max_silver_audit <=> MAX(audit_ts) Silver <=> the latest Bronze batch ingested into Silver
if max_audit_row["max_silver_audit_ts"] is None:
    # Silver empty → fallback so every Bronze row passes the filter
    max_silver_audit = "1900-01-01 00:00:00"
else:
    max_silver_audit = str(max_audit_row["max_silver_audit_ts"])

print(f"Max Silver audit_ts: {max_silver_audit}")


# current_audit_ts — frozen once per batch, written to every row inserted by this notebook. 
current_audit_ts = datetime.now()
print(f"Current Silver batch audit_ts: {current_audit_ts}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# BUILD tmp_silver — latest Silver state per key
# Silver is INSERT-only so each InvoiceID can have multiple history rows.
# We need the LATEST row per key to compare row_hash.
latest_per_key_window = Window.partitionBy(BUSINESS_KEY).orderBy(desc("audit_ts"))

tmp_silver = (
    silver_df
    .filter(col("deleted_audit_ts").isNull())
    .withColumn("version_rank", row_number().over(latest_per_key_window))
    .filter(col("version_rank") == 1)
    .drop("version_rank")
)

print(f"Latest Silver state (tmp_silver): {tmp_silver.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Differences vs FULL load (Customers/StockItems):
#   - Read ALL batches with audit_ts > marker (NOT just the latest batch)
#   - MUST dedup — same InvoiceID can appear across multiple Bronze days
#     because OLTP edited the row repeatedly. Keep latest LastEditedWhen.

# read all parquet (recursive into YYYY/MM/DD/)
bronze_df = spark.read.option("recursiveFileLookup", "true").parquet(BRONZE_PATH)


# filter rows NOT yet consumed by Silver
# - Only audit_ts > max_silver_audit
# - NO == max_bronze_audit (unlike FULL load — we need every newer batch)
tmp_bronze_raw = bronze_df.filter(col("audit_ts") > lit(max_silver_audit))
print(f"Rows after watermark filter: {tmp_bronze_raw.count()}")


# capture max Bronze audit_ts for watermark (log for etl.watermark)
max_bronze_audit_row = tmp_bronze_raw.agg(
    spark_max("audit_ts").alias("max_bronze_audit_ts")
).first()
max_bronze_audit = max_bronze_audit_row["max_bronze_audit_ts"]
print(f"Max Bronze audit_ts: {max_bronze_audit}")


# DEDUP — keep latest version per InvoiceID
# - PARTITION BY InvoiceID groups all versions of the same invoice
# - ORDER BY LastEditedWhen DESC puts the most recent edit first
# - Keep version_rank=1 → 1 row per InvoiceID (the latest edit)
#
# Example:
#   Bronze has InvoiceID=100 across 3 days (edited 3 times):
#   - 2026-05-13, LastEditedWhen=2026-05-13 10:00
#   - 2026-05-14, LastEditedWhen=2026-05-14 11:00
#   - 2026-05-15, LastEditedWhen=2026-05-15 12:00
#   Just keep the 2026-05-15 row only.
dedup_window = Window.partitionBy(BUSINESS_KEY).orderBy(desc(DEDUP_ORDER_COL))

# audit_ts <=> means when Bronze pipeline wrote this row to parquet files (@utcNow() at pipeline run time)
# LastEditedWhen <=> when source SQL edited, because the thing we need is the latest source state, not when did Bronze ingest this
tmp_bronze = (
    tmp_bronze_raw
    .withColumn("version_rank", row_number().over(dedup_window))
    .filter(col("version_rank") == 1)
    .drop("version_rank")
)
print(f"After DEDUP: {tmp_bronze.count()} rows")


# cast types to Silver schema
tmp_bronze = (
    tmp_bronze
    .withColumn("InvoiceID",                   col("InvoiceID").cast("int"))
    .withColumn("CustomerID",                  col("CustomerID").cast("int"))
    .withColumn("BillToCustomerID",            col("BillToCustomerID").cast("int"))
    .withColumn("OrderID",                     col("OrderID").cast("int"))
    .withColumn("DeliveryMethodID",            col("DeliveryMethodID").cast("int"))
    .withColumn("ContactPersonID",             col("ContactPersonID").cast("int"))
    .withColumn("AccountsPersonID",            col("AccountsPersonID").cast("int"))
    .withColumn("SalespersonPersonID",         col("SalespersonPersonID").cast("int"))
    .withColumn("PackedByPersonID",            col("PackedByPersonID").cast("int"))
    .withColumn("InvoiceDate",                 col("InvoiceDate").cast("date"))
    .withColumn("CustomerPurchaseOrderNumber", col("CustomerPurchaseOrderNumber").cast("string"))
    .withColumn("IsCreditNote",                col("IsCreditNote").cast("boolean"))
    .withColumn("CreditNoteReason",            col("CreditNoteReason").cast("string"))
    .withColumn("Comments",                    col("Comments").cast("string"))
    .withColumn("DeliveryInstructions",        col("DeliveryInstructions").cast("string"))
    .withColumn("InternalComments",            col("InternalComments").cast("string"))
    .withColumn("TotalDryItems",               col("TotalDryItems").cast("int"))
    .withColumn("TotalChillerItems",           col("TotalChillerItems").cast("int"))
    .withColumn("DeliveryRun",                 col("DeliveryRun").cast("string"))
    .withColumn("RunPosition",                 col("RunPosition").cast("string"))
    .withColumn("ReturnedDeliveryData",        col("ReturnedDeliveryData").cast("string"))
    .withColumn("ConfirmedDeliveryTime",       col("ConfirmedDeliveryTime").cast("timestamp"))
    .withColumn("ConfirmedReceivedBy",         col("ConfirmedReceivedBy").cast("string"))
    .withColumn("LastEditedBy",                col("LastEditedBy").cast("int"))
    .withColumn("LastEditedWhen",              col("LastEditedWhen").cast("timestamp"))
)


# compute row_hash
def safe_str(column_name):
    return coalesce(trim(col(column_name).cast("string")), lit("^"))

tmp_bronze = tmp_bronze.withColumn(
    "row_hash",
    sha2(concat_ws("|", *[safe_str(c) for c in BUSINESS_COLS]), 256)
)

print(f"Source ready (tmp_bronze): {tmp_bronze.count()} rows")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# INSERT CHANGED — row_hash differs → INSERT new version

changed_df = (
    tmp_bronze.alias("bronze")
    .join(
        tmp_silver.alias("silver"),
        on=col(f"bronze.{BUSINESS_KEY}") == col(f"silver.{BUSINESS_KEY}"),
        how="inner"
    )
    .filter(col("bronze.row_hash") != col("silver.row_hash"))
    .select(
        col(f"bronze.{BUSINESS_KEY}"),
        *[col(f"bronze.{c}") for c in BUSINESS_COLS],
        col("bronze.row_hash"),
        col("bronze.source_id")
    )
)

changed_to_insert = (
    changed_df
    .withColumn("audit_ts", lit(current_audit_ts).cast("timestamp"))
    .withColumn("deleted_audit_ts", lit(None).cast("timestamp"))
    .select(*ALL_SILVER_COLS)
)

updated_count = changed_to_insert.count()

if updated_count > 0:
    changed_to_insert.write.format("delta").mode("append").saveAsTable(SILVER_TABLE)

print(f"Inserted CHANGED versions: {updated_count} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# INSERT NEW — business_key not yet in Silver

new_df = (
    tmp_bronze.alias("bronze")
    .join(
        tmp_silver.alias("silver").select(BUSINESS_KEY),
        on=BUSINESS_KEY,
        how="left_anti"
    )
)

new_to_insert = (
    new_df
    .withColumn("audit_ts", lit(current_audit_ts).cast("timestamp"))
    .withColumn("deleted_audit_ts", lit(None).cast("timestamp"))
    .select(*ALL_SILVER_COLS)
)

new_count = new_to_insert.count()

if new_count > 0:
    new_to_insert.write.format("delta").mode("append").saveAsTable(SILVER_TABLE)

print(f"Inserted NEW keys: {new_count} rows")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# SAVE WATERMARK — log to etl.watermark

watermark_data = [(
    datetime.now(),
    WATERMARK_FLOW,
    str(max_bronze_audit)
)]

watermark_df = spark.createDataFrame(
    watermark_data,
    ["timestamp", "object_name", "watermark_value"]
)

watermark_df.write.format("delta").mode("append").saveAsTable("etl.watermark")
print(f"Watermark saved: {WATERMARK_FLOW} = {max_bronze_audit}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# VERIFY — count rows + sample
# Confirms Silver has correct data:
#   - Total rows (including history across runs)
#   - Latest active keys (version_rank=1, not soft-deleted)

silver_after = spark.read.table(SILVER_TABLE)

print(f"=== Total rows in Silver (history): {silver_after.count()} ===")

verify_window = Window.partitionBy(BUSINESS_KEY).orderBy(desc("audit_ts"))

latest_active = (
    silver_after
    .withColumn("version_rank", row_number().over(verify_window))
    .filter((col("version_rank") == 1) & col("deleted_audit_ts").isNull())
)
print(f"=== Latest active keys: {latest_active.count()} ===")

print("\n=== Sample 5 rows ===")
display(silver_after.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
