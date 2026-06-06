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

#   - Header (silver.wwi_invoices) + detail (silver.wwi_invoicelines) flatten
#   - Customer skey via SCD2 point-in-time join
#   - StockItem skey via SCD1 simple join
#   - DateKey derived from InvoiceDate
#   - Delete-then-insert idempotency

from pyspark.sql.functions import (
    col, lit, sha2, concat_ws, coalesce, trim,
    max as spark_max,
    row_number, desc,
    year, month, dayofmonth,
    date_format, expr
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime


# Source / target
SILVER_HEADER  = "silver.wwi_invoices"        # invoice header
SILVER_DETAIL  = "silver.wwi_invoicelines"    # invoice lines
DIM_CUSTOMER   = "gold.dim_customer"
DIM_STOCKITEM  = "gold.dim_stockitem"
GOLD_TABLE     = "gold.fact_invoiceline"
WATERMARK_FLOW = "silver_wwi_invoices_TO_gold_fact_invoiceline"

# Business key on fact = InvoiceLineID (line grain)
FACT_KEY       = "InvoiceLineID"

# Default skey for unresolved dim joins
DEFAULT_SKEY   = -1
DEFAULT_DATEKEY = -1
SOURCE_ID      = "WWI"

# Business cols used for row_hash (header + detail measures + dim skeys)
HASH_COLS_HEADER = [
    "InvoiceID", "CustomerID", "InvoiceDate",
    "CustomerPurchaseOrderNumber", "IsCreditNote"
]
HASH_COLS_DETAIL = [
    "InvoiceLineID", "StockItemID", "Description", "PackageTypeID",
    "Quantity", "UnitPrice", "TaxRate", "TaxAmount",
    "LineProfit", "ExtendedPrice"
]

# another table not original
HASH_COLS_SKEY = [
    "customer_skey", "stockitem_skey", "invoice_date_key"
]
HASH_COLS_ALL = HASH_COLS_HEADER + HASH_COLS_DETAIL + HASH_COLS_SKEY

print(f"Loading: ({SILVER_HEADER} + {SILVER_DETAIL}) → {GOLD_TABLE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# - max_fact_audit: when this flow last ran
# - current_audit_ts: this batch's time (frozen, applied to all new rows)
# - max_existing_skey: highest invoice_line_skey already used

fact_df = spark.read.table(GOLD_TABLE)

max_audit_row = fact_df.agg(
    spark_max("audit_ts").alias("max_fact_audit_ts")
).first()

if max_audit_row["max_fact_audit_ts"] is None:
    max_fact_audit = "1900-01-01 00:00:00"
else:
    max_fact_audit = str(max_audit_row["max_fact_audit_ts"])

print(f"Max Fact audit_ts: {max_fact_audit}")


current_audit_ts = datetime.now()
print(f"Current Fact batch audit_ts: {current_audit_ts}")


max_skey_row = fact_df.agg(spark_max("invoice_line_skey").alias("max_skey")).first()
max_existing_skey = max_skey_row["max_skey"] if max_skey_row["max_skey"] is not None else 0
print(f"Max existing invoice_line_skey: {max_existing_skey} (new skeys start at {max_existing_skey + 1})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# BUILD tmp_invoices — header latest active per InvoiceID
# Same DEDUP pattern as incremental Silver load.

invoices_silver = spark.read.table(SILVER_HEADER)

invoices_recent = (
    invoices_silver
    .filter(col("audit_ts") > lit(max_fact_audit))
    .filter(col("deleted_audit_ts").isNull())
)

inv_window = Window.partitionBy("InvoiceID").orderBy(desc("audit_ts"))

tmp_invoices = (
    invoices_recent
    .withColumn("version_rank", row_number().over(inv_window))
    .filter(col("version_rank") == 1)
    .drop("version_rank")
)

print(f"Header rows in tmp_invoices: {tmp_invoices.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# BUILD tmp_invoicelines — detail latest active per InvoiceLineID

invoicelines_silver = spark.read.table(SILVER_DETAIL)

invoicelines_recent = (
    invoicelines_silver
    .filter(col("audit_ts") > lit(max_fact_audit))
    .filter(col("deleted_audit_ts").isNull())
)

line_window = Window.partitionBy(FACT_KEY).orderBy(desc("audit_ts"))

tmp_invoicelines = (
    invoicelines_recent
    .withColumn("version_rank", row_number().over(line_window))
    .filter(col("version_rank") == 1)
    .drop("version_rank")
)

print(f"Detail rows in tmp_invoicelines: {tmp_invoicelines.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# FLATTEN + RESOLVE DIM SKEYS
# Take Silver header + detail, join them into line-grain rows, then resolve the 3 dim surrogate keys (customer, stockitem, date).
# Steps:
#   INNER JOIN header + detail on InvoiceID → line grain with header attrs
#   LEFT JOIN dim_customer (SCD2 point-in-time on InvoiceDate)
#     Condition: dim.CustomerID = h.CustomerID
#       AND h.InvoiceDate > dim.scd_from
#       AND h.InvoiceDate <= dim.scd_to
#   LEFT JOIN dim_stockitem (SCD1 simple join on StockItemID)
#   Derive invoice_date_key from InvoiceDate (YYYYMMDD int)
#   COALESCE all skeys to -1 if no match (so fact never has NULL FK)
#   Compute row_hash

# Load dims
dim_customer  = spark.read.table(DIM_CUSTOMER)
dim_stockitem = spark.read.table(DIM_STOCKITEM)


# header + detail flatten
header_detail = (
    tmp_invoices.alias("h")
    .join(
        tmp_invoicelines.alias("d"),
        on=col("h.InvoiceID") == col("d.InvoiceID"),
        how="inner"
    )
    .select(
        # natural keys
        col("h.InvoiceID").alias("InvoiceID"),
        col("d.InvoiceLineID").alias("InvoiceLineID"),
        # header attrs (carry to fact)
        col("h.CustomerID").alias("CustomerID"),
        col("h.InvoiceDate").alias("InvoiceDate"),
        col("h.CustomerPurchaseOrderNumber").alias("CustomerPurchaseOrderNumber"),
        col("h.IsCreditNote").alias("IsCreditNote"),
        # detail attrs + measures
        col("d.StockItemID").alias("StockItemID"),
        col("d.Description").alias("Description"),
        col("d.PackageTypeID").alias("PackageTypeID"),
        col("d.Quantity").alias("Quantity"),
        col("d.UnitPrice").alias("UnitPrice"),
        col("d.TaxRate").alias("TaxRate"),
        col("d.TaxAmount").alias("TaxAmount"),
        col("d.LineProfit").alias("LineProfit"),
        col("d.ExtendedPrice").alias("ExtendedPrice"),
    )
)


# SCD2 point-in-time join → customer_skey
flat_with_customer = (
    header_detail.alias("f")
    .join(
        dim_customer.alias("dc"),
        (col("dc.CustomerID") == col("f.CustomerID")) &
        (col("f.InvoiceDate") > col("dc.scd_from")) &
        (col("f.InvoiceDate") <= col("dc.scd_to"))
        how="left"
    )
    .select( 
        col("f.*"),
        coalesce(col("dc.customer_skey"), -1)
    )
)


# SCD1 simple join → stockitem_skey
flat_with_stockitem = (
    flat_with_customer.alias("f")
    .join(
        dim_stockitem.alias("ds"),
        (col("ds.StockItemID") == col("f.StockItemID")) &
        (col("ds.stockitem_skey") != DEFAULT_SKEY),
        how="left"
    )
    .select(
        col("f.*"),
        col("ds.stockitem_skey")
    )
)


# derive invoice_date_key + COALESCE skeys to -1
flat_final = (
    flat_with_stockitem
    # Date key from InvoiceDate: YYYYMMDD
    .withColumn(
        "invoice_date_key",
        coalesce(
            (year("InvoiceDate") * 10000 + month("InvoiceDate") * 100 + dayofmonth("InvoiceDate")).cast("int"),
            lit(DEFAULT_DATEKEY)
        )
    )
    # Skey COALESCE to -1 for unmatched
    .withColumn("customer_skey",  coalesce(col("customer_skey"),  lit(DEFAULT_SKEY)).cast("int"))
    .withColumn("stockitem_skey", coalesce(col("stockitem_skey"), lit(DEFAULT_SKEY)).cast("int"))
)


# compute row_hash
def safe_str(column_name):
    return coalesce(trim(col(column_name).cast("string")), lit("^"))

flat_final = flat_final.withColumn(
    "row_hash",
    sha2(concat_ws("|", *[safe_str(c) for c in HASH_COLS_ALL]), 256)
)

print(f"Flattened + dim-resolved rows: {flat_final.count()}")

# Quick sanity check: how many rows have unresolved skeys (= -1)?
unresolved_customer  = flat_final.filter(col("customer_skey")  == DEFAULT_SKEY).count()
unresolved_stockitem = flat_final.filter(col("stockitem_skey") == DEFAULT_SKEY).count()
print(f"Unresolved customer_skey  (= -1): {unresolved_customer}")
print(f"Unresolved stockitem_skey (= -1): {unresolved_stockitem}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# DELETE existing fact rows via Delta MERGE (production-grade)
# Distributed — no .collect() to driver, scales to millions of rows.
# Before inserting new fact rows, delete any existing fact rows that share the same InvoiceLineID as our batch.
# This is the "delete" half of the delete-then-insert pattern.
if flat_final.count() > 0:
    fact_delta = DeltaTable.forName(spark, GOLD_TABLE)

    # Build a small DataFrame of just the keys to delete (distinct, distributed)
    keys_to_delete = flat_final.select(FACT_KEY).distinct()

    # MERGE: match by key, delete matched rows
    fact_delta.alias("tgt").merge(
        keys_to_delete.alias("src"),
        f"tgt.{FACT_KEY} = src.{FACT_KEY}"
    ).whenMatchedDelete().execute()

    print(f"Deleted existing fact rows matching {keys_to_delete.count()} InvoiceLineIDs")
else:
    print("No InvoiceLineIDs to delete")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ALLOCATE invoice_line_skey + INSERT into fact
# - Skey starts at max_existing_skey + 1, assigned via row_number()
# - Uses LongType (more headroom than IntegerType for fact volumes)

total_to_insert = flat_final.count()

if total_to_insert > 0:
    skey_window = Window.orderBy(FACT_KEY)

    fact_with_skey = (
        flat_final
        .withColumn(
            "invoice_line_skey",
            (lit(max_existing_skey) + row_number().over(skey_window)).cast("long")
        )
        .withColumn("audit_ts",  lit(current_audit_ts).cast("timestamp"))
        .withColumn("source_id", lit(SOURCE_ID))
    )

    # Final column order — must match DDL
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
    print("No rows to insert — flat_final is empty")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# SAVE WATERMARK + VERIFY

# Watermark value = max audit_ts consumed from Silver (header table, drives this flow)
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
    print("No new Silver header rows consumed — skipping watermark save")


# Verify
result = spark.read.table(GOLD_TABLE)

total            = result.count()
unresolved_cust  = result.filter(col("customer_skey")  == DEFAULT_SKEY).count()
unresolved_stk   = result.filter(col("stockitem_skey") == DEFAULT_SKEY).count()
unresolved_date  = result.filter(col("invoice_date_key") == DEFAULT_DATEKEY).count()

total_revenue    = result.agg(
    spark_max("ExtendedPrice").alias("max_ext"),
    spark_max("Quantity").alias("max_qty")
).first()

revenue_sum_df = spark.sql(f"""
    SELECT
        SUM(ExtendedPrice) AS total_extended_price,
        SUM(Quantity)      AS total_quantity,
        COUNT(DISTINCT InvoiceID) AS unique_invoices,
        COUNT(DISTINCT customer_skey) AS unique_customers,
        COUNT(DISTINCT stockitem_skey) AS unique_stockitems
    FROM {GOLD_TABLE}
""")

print(f"\n=== Total fact rows: {total} ===")
print(f"Unresolved customer_skey  (= -1): {unresolved_cust}")
print(f"Unresolved stockitem_skey (= -1): {unresolved_stk}")
print(f"Unresolved invoice_date_key (= -1): {unresolved_date}")

print("\n=== Revenue summary ===")
display(revenue_sum_df)

print("\n=== Sample 5 fact rows ===")
display(
    result.select(
        "invoice_line_skey", "InvoiceID", "InvoiceLineID",
        "customer_skey", "stockitem_skey", "invoice_date_key",
        "InvoiceDate", "Quantity", "ExtendedPrice"
    )
    .orderBy("invoice_line_skey")
    .limit(5)
)


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
