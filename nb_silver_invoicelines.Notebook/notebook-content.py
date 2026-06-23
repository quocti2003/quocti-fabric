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
    row_number, desc
)
from pyspark.sql.window import Window
from datetime import datetime


BRONZE_PATH    = "Files/bronze/wwi_invoicelines/"
SILVER_TABLE   = "silver.wwi_invoicelines"
BUSINESS_KEY   = "InvoiceLineID"
WATERMARK_FLOW = "bronze_wwi_invoicelines_TO_silver_wwi_invoicelines"

DEDUP_ORDER_COL = "LastEditedWhen"

BUSINESS_COLS = [
    "InvoiceID", "StockItemID", "Description", "PackageTypeID",
    "Quantity", "UnitPrice", "TaxRate", "TaxAmount",
    "LineProfit", "ExtendedPrice", "LastEditedBy", "LastEditedWhen"
]

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

# max_silver_audit <=> the latest Bronze batch ingested into Silver table
if max_audit_row["max_silver_audit_ts"] is None:
    max_silver_audit = "1900-01-01 00:00:00"
else:
    max_silver_audit = str(max_audit_row["max_silver_audit_ts"])

print(f"Max Silver audit_ts: {max_silver_audit}")

current_audit_ts = datetime.now()
print(f"Current Silver batch audit_ts: {current_audit_ts}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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

# tmp_bronze — INCREMENTAL read + DEDUP + cast + row_hash
# Read all batches with audit_ts > max_silver_audit, then keep latest version per
# InvoiceLineID via Window(PARTITION BY key ORDER BY LastEditedWhen DESC).

# read all parquet (recursive)
bronze_df = spark.read.option("recursiveFileLookup", "true").parquet(BRONZE_PATH)


# filter rows NOT yet consumed to Silver
tmp_bronze_raw = bronze_df.filter(col("audit_ts") > lit(max_silver_audit))
print(f"Rows after watermark filter: {tmp_bronze_raw.count()}")


# capture max Bronze audit_ts for watermark save
max_bronze_audit_row = tmp_bronze_raw.agg(
    spark_max("audit_ts").alias("max_bronze_audit_ts")
).first()
max_bronze_audit = max_bronze_audit_row["max_bronze_audit_ts"]
print(f"Max Bronze audit_ts: {max_bronze_audit}")


# DEDUP — keep latest version per InvoiceLineID
dedup_window = Window.partitionBy(BUSINESS_KEY).orderBy(desc(DEDUP_ORDER_COL))

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
    .withColumn("InvoiceLineID",  col("InvoiceLineID").cast("int"))
    .withColumn("InvoiceID",      col("InvoiceID").cast("int"))
    .withColumn("StockItemID",    col("StockItemID").cast("int"))
    .withColumn("Description",    col("Description").cast("string"))
    .withColumn("PackageTypeID",  col("PackageTypeID").cast("int"))
    .withColumn("Quantity",       col("Quantity").cast("int"))
    .withColumn("UnitPrice",      col("UnitPrice").cast("decimal(18,2)"))
    .withColumn("TaxRate",        col("TaxRate").cast("decimal(18,3)"))
    .withColumn("TaxAmount",      col("TaxAmount").cast("decimal(18,2)"))
    .withColumn("LineProfit",     col("LineProfit").cast("decimal(18,2)"))
    .withColumn("ExtendedPrice",  col("ExtendedPrice").cast("decimal(18,2)"))
    .withColumn("LastEditedBy",   col("LastEditedBy").cast("int"))
    .withColumn("LastEditedWhen", col("LastEditedWhen").cast("timestamp"))
)


# compute row_hash — SHA256( COALESCE(TRIM(col), '^') | ... )
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

# CELL ********************

spark.sql("""
SELECT object_name, COUNT(*) AS run_count, MAX(timestamp) AS last_run
FROM etl.watermark
WHERE object_name LIKE 'bronze_wwi_%'
GROUP BY object_name
ORDER BY object_name
""").show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
