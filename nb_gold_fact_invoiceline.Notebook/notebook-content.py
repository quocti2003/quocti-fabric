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

# This notebook takes invoice header + line data from Silver, joins them together, 
# looks up the correct dimension keys from dim_customer/dim_stockitem/dim_date, and 
# writes the result into gold.fact_invoiceline — the central fact table for BI to analyze sales.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
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


# Source / target
SILVER_HEADER  = "silver.wwi_invoices"
SILVER_DETAIL  = "silver.wwi_invoicelines"
DIM_CUSTOMER   = "gold.dim_customer"
DIM_STOCKITEM  = "gold.dim_stockitem"
GOLD_TABLE     = "gold.fact_invoiceline"
WATERMARK_FLOW = "silver_wwi_invoices_TO_gold_fact_invoiceline"

# Business key on fact
FACT_KEY        = "InvoiceLineID"

# Sentinels
DEFAULT_SKEY    = -1
DEFAULT_DATEKEY = -1
SOURCE_ID       = "WWI"

# Hash cols
HASH_COLS_HEADER = [
    "InvoiceID", "CustomerID", "InvoiceDate",
    "CustomerPurchaseOrderNumber", "IsCreditNote"
]
HASH_COLS_DETAIL = [
    "InvoiceLineID", "StockItemID", "Description", "PackageTypeID",
    "Quantity", "UnitPrice", "TaxRate", "TaxAmount",
    "LineProfit", "ExtendedPrice"
]
HASH_COLS_SKEY = ["customer_skey", "stockitem_skey", "invoice_date_key"]
HASH_COLS_ALL  = HASH_COLS_HEADER + HASH_COLS_DETAIL + HASH_COLS_SKEY

print(f"Loading: ({SILVER_HEADER} + {SILVER_DETAIL}) → {GOLD_TABLE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# PHASE 1 — Read CURRENT Fact state
# ============================================================
fact_before_insert = spark.read.table(GOLD_TABLE)

# (1) Marker
max_fact_audit = (
    fact_before_insert.agg(spark_max("audit_ts").alias("m")).first()["m"]
)
if max_fact_audit is None:
    max_fact_audit = "1900-01-01 00:00:00"
else:
    max_fact_audit = str(max_fact_audit)
print(f"Max Fact audit_ts (marker): {max_fact_audit}")

# (2) Max existing skey
max_existing_skey = (
    fact_before_insert.agg(spark_max("invoice_line_skey").alias("m")).first()["m"]
)
if max_existing_skey is None:
    max_existing_skey = 0
print(f"Max existing invoice_line_skey: {max_existing_skey}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# PHASE 2 — Read Silver delta (invoices + invoicelines)
# ============================================================
# FIX: Rank FIRST, then filter deleted AFTER (same pattern as dim_customer)

# ─── Invoices header ───
silver_invoices = spark.read.table(SILVER_HEADER)
inv_recent = silver_invoices.filter(col("audit_ts") > lit(max_fact_audit))

inv_window = Window.partitionBy("InvoiceID").orderBy(desc("audit_ts"))
tmp_invoices = (
    inv_recent
    .withColumn("version_rank", row_number().over(inv_window))
    .filter(col("version_rank") == 1)
    .filter(col("deleted_audit_ts").isNull())          # ← FIX: filter AFTER rank
    .drop("version_rank")
)
print(f"Latest invoices since marker: {tmp_invoices.count()} rows")


# ─── InvoiceLines detail ───
silver_lines = spark.read.table(SILVER_DETAIL)
line_recent = silver_lines.filter(col("audit_ts") > lit(max_fact_audit))

line_window = Window.partitionBy("InvoiceLineID").orderBy(desc("audit_ts"))
tmp_invoicelines = (
    line_recent
    .withColumn("version_rank", row_number().over(line_window))
    .filter(col("version_rank") == 1)
    .filter(col("deleted_audit_ts").isNull())          # ← FIX: filter AFTER rank
    .drop("version_rank")
)
print(f"Latest invoicelines since marker: {tmp_invoicelines.count()} rows")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# PHASE 3 — Flatten header+detail + Resolve dim skeys + row_hash
# ============================================================

# (1) Header + detail INNER JOIN
header_detail = (
    tmp_invoices.alias("h")
    .join(
        tmp_invoicelines.alias("d"),
        on=col("h.InvoiceID") == col("d.InvoiceID"),
        how="inner"
    )
    .select(
        col("d.InvoiceLineID"),
        col("h.InvoiceID"),
        col("h.CustomerID"),
        col("h.InvoiceDate"),
        col("h.CustomerPurchaseOrderNumber"),
        col("h.IsCreditNote"),
        col("d.StockItemID"),
        col("d.Description"),
        col("d.PackageTypeID"),
        col("d.Quantity"),
        col("d.UnitPrice"),
        col("d.TaxRate"),
        col("d.TaxAmount"),
        col("d.LineProfit"),
        col("d.ExtendedPrice")
    )
)

# (2) Resolve customer_skey via SCD2 point-in-time
dim_customer = spark.read.table(DIM_CUSTOMER) \
    .filter(col("customer_skey") != DEFAULT_SKEY) \
    .select("customer_skey", "CustomerID", "scd_from", "scd_to")

flat_with_customer = (
    header_detail.alias("f")
    .join(
        dim_customer.alias("dc"),
        (col("dc.CustomerID") == col("f.CustomerID")) &
        (col("f.InvoiceDate") > col("dc.scd_from")) &
        (col("f.InvoiceDate") <= col("dc.scd_to")),     # ← FIX: comma added
        how="left"
    )
    .select(
        col("f.*"),
        coalesce(col("dc.customer_skey"), lit(DEFAULT_SKEY)).cast("int").alias("customer_skey")
    )
)

# (3) Resolve stockitem_skey via SCD1 simple join
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

# (4) Derive invoice_date_key (YYYYMMDD)
flat_with_datekey = flat_with_stockitem.withColumn(
    "invoice_date_key",
    (year("InvoiceDate") * 10000 + month("InvoiceDate") * 100 + dayofmonth("InvoiceDate")).cast("int")
)

# (5) Compute row_hash
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

# ============================================================
# PHASE 4 — Stamp batch + Delete-then-insert pattern
# ============================================================

current_audit_ts = datetime.now()
print(f"Current Fact batch audit_ts: {current_audit_ts}")


if total_to_insert > 0:
    # ─── Step 1: DELETE existing fact rows in batch (idempotent) ───
    delta_fact = DeltaTable.forName(spark, GOLD_TABLE)
    delta_fact.alias("tgt").merge(
        flat_final.select(FACT_KEY).alias("src"),
        f"tgt.{FACT_KEY} = src.{FACT_KEY}"
    ).whenMatchedDelete().execute()
    print(f"Deleted prior fact rows for {total_to_insert} InvoiceLineIDs")


    # ─── Step 2: Allocate skeys + add audit cols ───
    skey_window = Window.orderBy(FACT_KEY)

    fact_with_skey = (
        flat_final
        .withColumn("invoice_line_skey",
                    (lit(max_existing_skey) + row_number().over(skey_window)).cast("long"))
        .withColumn("audit_ts",  lit(current_audit_ts).cast("timestamp"))
        .withColumn("source_id", lit(SOURCE_ID))
    )

    # ─── Step 3: Reorder cols + INSERT ───
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

# ============================================================
# PHASE 5 — Save watermark + Verify
# ============================================================
max_silver_audit_consumed = (
    spark.read.table(SILVER_HEADER)
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
