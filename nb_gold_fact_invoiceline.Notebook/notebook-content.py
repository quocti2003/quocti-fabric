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

from pyspark.sql.functions import (
    col, lit, sha2, concat_ws, coalesce, trim,
    max as spark_max,
    row_number, desc,
    year, month, dayofmonth, date_format, expr
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime

# Source / target tables
SILVER_INVOICES     = "silver.wwi_invoices"
SILVER_INVOICELINES = "silver.wwi_invoicelines"
DIM_CUSTOMER        = "gold.dim_customer"
DIM_STOCKITEM       = "gold.dim_stockitem"
GOLD_TABLE          = "gold.fact_invoiceline"
WATERMARK_FLOW      = "silver_wwi_invoices_TO_gold_fact_invoiceline"

# Business key on fact = line grain
FACT_KEY            = "InvoiceLineID"

# Sentinels for unresolved dim joins
DEFAULT_SKEY        = -1
DEFAULT_DATEKEY     = -1
SOURCE_ID           = "WWI"

# row_hash columns grouped by source table
HASH_COLS_INVOICES = [
    "InvoiceID", "CustomerID", "InvoiceDate",
    "CustomerPurchaseOrderNumber", "IsCreditNote"
]
HASH_COLS_INVOICELINES = [
    "InvoiceLineID", "StockItemID", "Description", "PackageTypeID",
    "Quantity", "UnitPrice", "TaxRate", "TaxAmount",
    "LineProfit", "ExtendedPrice"
]
HASH_COLS_SKEY = ["customer_skey", "stockitem_skey", "invoice_date_key"]
HASH_COLS_ALL  = HASH_COLS_INVOICES + HASH_COLS_INVOICELINES + HASH_COLS_SKEY

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read CURRENT Fact state (marker only)
fact_before_insert = spark.read.table(GOLD_TABLE)

# Output: max_fact_audit — lower bound for Silver filter 
max_fact_audit = (
    fact_before_insert.agg(spark_max("audit_ts").alias("m")).first()["m"]
)
if max_fact_audit is None:
    max_fact_audit = "1900-01-01 00:00:00"
else:
    max_fact_audit = str(max_fact_audit)

print(f"Max Fact audit_ts (marker): {max_fact_audit}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read Silver delta (invoices + invoicelines)

# Invoices (silver.wwi_invoices) 
silver_invoices_df = spark.read.table(SILVER_INVOICES)
inv_recent = silver_invoices_df.filter(col("audit_ts") > lit(max_fact_audit)) # must newer than the last time the Gold consume Silver

inv_window = Window.partitionBy("InvoiceID").orderBy(desc("audit_ts"))
tmp_invoices = (
    inv_recent
    .withColumn("version_rank", row_number().over(inv_window))
    .filter(col("version_rank") == 1)
    .filter(col("deleted_audit_ts").isNull())
    .drop("version_rank")
)
print(f"Latest current active invoices since marker: {tmp_invoices.count()} rows")


# InvoiceLines (silver.wwi_invoicelines)
silver_invoicelines_df = spark.read.table(SILVER_INVOICELINES)
line_recent = silver_invoicelines_df.filter(col("audit_ts") > lit(max_fact_audit))

line_window = Window.partitionBy("InvoiceLineID").orderBy(desc("audit_ts"))
tmp_invoicelines = (
    line_recent
    .withColumn("version_rank", row_number().over(line_window))
    .filter(col("version_rank") == 1)
    .filter(col("deleted_audit_ts").isNull())
    .drop("version_rank")
)
print(f"Latest current active invoicelines since marker: {tmp_invoicelines.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Flatten invoices+invoicelines + Resolve dim skeys + row_hash

# Flatten: INNER JOIN tmp_invoices ∩ tmp_invoicelines → line grain
# Invoice cols denormalized into each line row (CustomerID, InvoiceDate, etc.)
# invoices_lines_flat = detail rows enriched with invoice info
# Fact table = line grain = 1 row per InvoiceLineID, Reports query at line level but need invoice context (who bought, when):
invoices_lines_flat = (
    tmp_invoices.alias("inv")
    .join(
        tmp_invoicelines.alias("ln"),
        on=col("inv.InvoiceID") == col("ln.InvoiceID"),
        how="inner"
    )
    .select(
        col("ln.InvoiceLineID"),
        col("inv.InvoiceID"),
        col("inv.CustomerID"),
        col("inv.InvoiceDate"),
        col("inv.CustomerPurchaseOrderNumber"),
        col("inv.IsCreditNote"),
        col("ln.StockItemID"),
        col("ln.Description"),
        col("ln.PackageTypeID"),
        col("ln.Quantity"),
        col("ln.UnitPrice"),
        col("ln.TaxRate"),
        col("ln.TaxAmount"),
        col("ln.LineProfit"),
        col("ln.ExtendedPrice")
    )
)


# Resolve customer_skey via SCD2 point-in-time
# Take invoices_lines_flat (line-grain fact rows) and add customer_skey column by joining with dim_customer using SCD2 point-in-time logic.
dim_customer = spark.read.table(DIM_CUSTOMER) \
    .filter(col("customer_skey") != DEFAULT_SKEY) \
    .select("customer_skey", "CustomerID", "scd_from", "scd_to")

flat_with_customer = (
    invoices_lines_flat.alias("f")
    .join(
        dim_customer.alias("dc"),
        (col("dc.CustomerID") == col("f.CustomerID")) &
        (col("f.InvoiceDate") > col("dc.scd_from")) &
        (col("f.InvoiceDate") <= col("dc.scd_to")),
        how="left"
    )
    .select(
        col("f.*"),
        coalesce(col("dc.customer_skey"), lit(DEFAULT_SKEY)).cast("int").alias("customer_skey")
    )
)


# Resolve stockitem_skey via SCD1 simple join
dim_stockitem = spark.read.table(DIM_STOCKITEM) \
    .filter(col("stockitem_skey") != DEFAULT_SKEY) \
    .select("stockitem_skey", "StockItemID")

flat_with_stockitem = (
    flat_with_customer.alias("f")
    .join(
        dim_stockitem.alias("ds"),
        on=col("f.StockItemID") == col("ds.StockItemID"),
        how="left"
    )
    .select(
        col("f.*"),
        coalesce(col("ds.stockitem_skey"), lit(DEFAULT_SKEY)).cast("int").alias("stockitem_skey")
    )
)


# Derive invoice_date_key (YYYYMMDD as int)
flat_with_datekey = flat_with_stockitem.withColumn(
    "invoice_date_key",
    (year("InvoiceDate") * 10000 + month("InvoiceDate") * 100 + dayofmonth("InvoiceDate")).cast("int")
)


# Compute row_hash
def safe_str(c):
    return coalesce(trim(col(c).cast("string")), lit("^"))

flat_final = flat_with_datekey.withColumn(
    "row_hash",
    sha2(concat_ws("|", *[safe_str(c) for c in HASH_COLS_ALL]), 256)
)

total_to_insert = flat_final.count()
print(f"Fact rows to load: {total_to_insert}")

# Sanity check unresolved skeys
unresolved_customer  = flat_final.filter(col("customer_skey")  == DEFAULT_SKEY).count()
unresolved_stockitem = flat_final.filter(col("stockitem_skey") == DEFAULT_SKEY).count()
print(f"Unresolved customer_skey:  {unresolved_customer}")
print(f"Unresolved stockitem_skey: {unresolved_stockitem}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Stamp batch + Delete prior + Allocate skey + INSERT
# Stamp this batch
current_audit_ts = datetime.now()
print(f"Current Fact batch audit_ts: {current_audit_ts}")


# Get max existing skey 
max_existing_skey = (
    spark.read.table(GOLD_TABLE)
    .agg(spark_max("invoice_line_skey").alias("m"))
    .first()["m"]
)
if max_existing_skey is None:
    max_existing_skey = 0
print(f"Max existing invoice_line_skey: {max_existing_skey}")


if total_to_insert > 0:
    # DELETE existing fact rows in batch (idempotent) 
    delta_fact = DeltaTable.forName(spark, GOLD_TABLE)
    delta_fact.alias("tgt").merge(
        flat_final.select(FACT_KEY).alias("src"),
        f"tgt.{FACT_KEY} = src.{FACT_KEY}"
    ).whenMatchedDelete().execute()
    print(f"Deleted prior fact rows for {total_to_insert} InvoiceLineIDs")



    # Allocate skeys + add audit cols
    skey_window = Window.orderBy(FACT_KEY)

    fact_with_skey = (
        flat_final
        .withColumn("invoice_line_skey",
                    (lit(max_existing_skey) + row_number().over(skey_window)).cast("long"))
        .withColumn("audit_ts",  lit(current_audit_ts).cast("timestamp"))
        .withColumn("source_id", lit(SOURCE_ID))
    )


    # Reorder cols + INSERT
    ALL_FACT_COLS = [
        "invoice_line_skey",
        "InvoiceID", "InvoiceLineID",
        "customer_skey", "stockitem_skey", "invoice_date_key",
        "CustomerID", "StockItemID",
        "InvoiceDate", "CustomerPurchaseOrderNumber", "IsCreditNote",
        "Description", "PackageTypeID",
        "Quantity", "UnitPrice", "TaxRate", "TaxAmount",
        "LineProfit", "ExtendedPrice",
        "audit_ts", "source_id", "row_hash"
    ]

    fact_with_skey.select(*ALL_FACT_COLS) \
        .write.format("delta").mode("append").saveAsTable(GOLD_TABLE)
    print(f"Inserted {total_to_insert} fact rows "
          f"(skeys {max_existing_skey + 1} → {max_existing_skey + total_to_insert})")
else:
    print("No new fact rows to load")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Save watermark + Verify

max_silver_audit_consumed = (
    spark.read.table(SILVER_INVOICES)
    .filter(col("audit_ts") > lit(max_fact_audit))
    .agg(spark_max("audit_ts").alias("m"))
    .first()["m"]
)

if max_silver_audit_consumed is not None:
    watermark_df = spark.createDataFrame(
        [(datetime.now(), WATERMARK_FLOW, str(max_silver_audit_consumed))],
        ["timestamp", "object_name", "watermark_value"]
    )
    watermark_df.write.format("delta").mode("append").saveAsTable("etl.watermark")
    print(f"Watermark saved: {WATERMARK_FLOW} = {max_silver_audit_consumed}")
else:
    print("No new Silver rows consumed — skipping watermark save")


# Verify
result = spark.read.table(GOLD_TABLE)
total = result.count()
unresolved_cust = result.filter(col("customer_skey") == DEFAULT_SKEY).count()
unresolved_stk  = result.filter(col("stockitem_skey") == DEFAULT_SKEY).count()
unresolved_date = result.filter(col("invoice_date_key") == DEFAULT_DATEKEY).count()
revenue = result.agg({"ExtendedPrice": "sum"}).first()[0]

print(f"\n=== Total fact rows: {total} ===")
print(f"Unresolved customer_skey:    {unresolved_cust}")
print(f"Unresolved stockitem_skey:   {unresolved_stk}")
print(f"Unresolved invoice_date_key: {unresolved_date}")
print(f"Total ExtendedPrice (revenue): {revenue}")

display(result.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
SELECT object_name, COUNT(*) AS run_count, MAX(timestamp) AS last_run
FROM etl.watermark
WHERE object_name LIKE '%dim_customer%'
   OR object_name LIKE '%dim_stockitem%'
   OR object_name LIKE '%fact_invoiceline%'
GROUP BY object_name
ORDER BY object_name
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
