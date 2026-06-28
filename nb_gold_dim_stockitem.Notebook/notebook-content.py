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

# Key difference vs SCD2: row_hash differs → UPDATE existing row,
# don't INSERT a new version. No timeline recompute needed.
from pyspark.sql.functions import (
    col, lit, sha2, concat_ws, coalesce, trim,
    max as spark_max,
    row_number, desc
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime

SILVER_TABLE   = "silver.wwi_stockitems"
GOLD_TABLE     = "gold.dim_stockitem"
BUSINESS_KEY   = "StockItemID"
WATERMARK_FLOW = "silver_wwi_stockitems_TO_gold_dim_stockitem"

# Business cols — used for row_hash + UPDATE/INSERT column lists
BUSINESS_COLS = [
    "StockItemName", "SupplierID", "ColorID", "UnitPackageID",
    "Brand", "Size", "TaxRate", "UnitPrice", "RecommendedRetailPrice",
    "Barcode", "Tags", "CustomFields", "SearchDetails", "LastEditedBy"
]

# Sentinels (default row + SCD constants for SCD1)
DEFAULT_SKEY    = -1
DEFAULT_VERSION = 1 # SCD1: always 1, because update in-place overwrite
DEFAULT_ACTIVE  = 1 # SCD1: always 1
INFERRED_FALSE  = 0 # Real data — normal dim row, INFERRED_TRUE = 1 this one means late-arriving placeholder, temporary row, business cols = NULL/default
SOURCE_ID       = "WWI"
SCD_TO_INFINITY = datetime(9999, 12, 31)

# print(f"Loading: {SILVER_TABLE} → {GOLD_TABLE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read CURRENT Gold state
# Outputs: gold_before_insert, max_gold_audit, max_existing_skey, tmp_dim

gold_before_insert = spark.read.table(GOLD_TABLE)

# max_gold_audit — marker, lower bound for Silver filter 
max_gold_audit = (
    gold_before_insert.agg(spark_max("audit_ts").alias("m")).first()["m"]
)
if max_gold_audit is None:
    max_gold_audit = "1900-01-01 00:00:00"
else:
    max_gold_audit = str(max_gold_audit)
print(f"Max Gold audit_ts (marker): {max_gold_audit}") # max_gold_audit = the latest timestamp Gold ingested Silver


# max_existing_skey — highest skey used (exclude -1 default)
max_existing_skey = (
    gold_before_insert
    .filter(col("stockitem_skey") != DEFAULT_SKEY)
    .agg(spark_max("stockitem_skey").alias("m")).first()["m"]
)
if max_existing_skey is None:
    max_existing_skey = 0
print(f"Max existing stockitem_skey: {max_existing_skey} (new skeys start at {max_existing_skey + 1})")

# tmp_dim — current dim per StockItemID (SCD1 = 1 row per key, no scd_active filter)
tmp_dim = gold_before_insert.filter(col("stockitem_skey") != DEFAULT_SKEY)
print(f"Current dim rows (tmp_dim): {tmp_dim.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read Silver delta + compute row_hash
# Rank FIRST (over all versions), then filter deleted AFTER
# → If latest version is deleted, drop entire key 

silver_df = spark.read.table(SILVER_TABLE)

# Only filter watermark here (NOT deleted yet)
silver_recent = silver_df.filter(col("audit_ts") > lit(max_gold_audit))

# Window: latest version per StockItemID
latest_window = Window.partitionBy(BUSINESS_KEY).orderBy(desc("audit_ts"))

tmp_silver = (
    silver_recent
    .withColumn("version_rank", row_number().over(latest_window))
    .filter(col("version_rank") == 1)
    .filter(col("deleted_audit_ts").isNull())          
    .drop("version_rank")
)

# Compute row_hash
def safe_str(c):
    return coalesce(trim(col(c).cast("string")), lit("^"))

tmp_silver = tmp_silver.withColumn(
    "row_hash",
    sha2(concat_ws("|", *[safe_str(c) for c in BUSINESS_COLS]), 256)
)
print(f"Latest Silver per StockItemID (tmp_silver): {tmp_silver.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Identify rows to MERGE (CHANGED) + INSERT (NEW)
# CHANGED: row in both + hash differs
changed_rows = (
    tmp_silver.alias("silver")
    .join(
        tmp_dim.alias("dim"),
        on=col(f"silver.{BUSINESS_KEY}") == col(f"dim.{BUSINESS_KEY}"),
        how="inner"
    )
    .filter(col("silver.row_hash") != col("dim.row_hash"))
    .select(
        col(f"silver.{BUSINESS_KEY}"),
        *[col(f"silver.{c}") for c in BUSINESS_COLS],
        col("silver.row_hash")
    )
)

# NEW: in Silver, not in dim
new_rows = (
    tmp_silver.alias("silver")
    .join(
        tmp_dim.alias("dim").select(BUSINESS_KEY),
        on=BUSINESS_KEY,
        how="left_anti"
    )
    .select(
        col(BUSINESS_KEY),
        *[col(c) for c in BUSINESS_COLS],
        col("row_hash")
    )
)

changed_count = changed_rows.count()
new_count = new_rows.count()
print(f"CHANGED rows (MERGE UPDATE): {changed_count}")
print(f"NEW rows (INSERT): {new_count}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Stamp batch + Allocate skeys + UPDATE (CHANGED) + INSERT (NEW)
# SCD1 pattern: UPDATE in place (no history). Bump updated_audit_ts.

current_audit_ts = datetime.now()
print(f"Current batch audit_ts: {current_audit_ts}")


# ─── UPDATE CHANGED via Delta MERGE ───
if changed_count > 0:
    delta_dim = DeltaTable.forName(spark, GOLD_TABLE)

    update_set = {c: f"src.{c}" for c in BUSINESS_COLS}
    update_set["row_hash"] = "src.row_hash"
    update_set["updated_audit_ts"] = f"timestamp('{current_audit_ts}')"

    delta_dim.alias("tgt").merge(
        changed_rows.alias("src"),
        f"tgt.{BUSINESS_KEY} = src.{BUSINESS_KEY}"
    ).whenMatchedUpdate(set=update_set).execute()
    print(f"Updated CHANGED: {changed_count} rows")


# ─── INSERT NEW with allocated skeys ───
if new_count > 0:
    skey_window = Window.orderBy(BUSINESS_KEY)

    new_with_skey = (
        new_rows
        .withColumn("stockitem_skey",
                    (lit(max_existing_skey) + row_number().over(skey_window)).cast("int"))
        .withColumn("scd_from",        lit(current_audit_ts).cast("timestamp"))
        .withColumn("scd_to",          lit(SCD_TO_INFINITY).cast("timestamp"))
        .withColumn("scd_version",     lit(DEFAULT_VERSION).cast("int"))
        .withColumn("scd_active",      lit(DEFAULT_ACTIVE).cast("int"))
        .withColumn("inferred_flag",   lit(INFERRED_FALSE).cast("int"))
        .withColumn("audit_ts",        lit(current_audit_ts).cast("timestamp"))
        .withColumn("updated_audit_ts", lit(None).cast("timestamp"))
        .withColumn("source_id",       lit(SOURCE_ID))
    )

    ALL_GOLD_COLS = [
        "stockitem_skey", BUSINESS_KEY, *BUSINESS_COLS,
        "scd_from", "scd_to", "scd_version", "scd_active", "inferred_flag",
        "audit_ts", "updated_audit_ts", "source_id", "row_hash"
    ]

    new_with_skey.select(*ALL_GOLD_COLS) \
        .write.format("delta").mode("append").saveAsTable(GOLD_TABLE)
    print(f"Inserted NEW: {new_count} rows (skeys {max_existing_skey + 1} → {max_existing_skey + new_count})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Ensure default row (stockitem_skey = -1)
default_exists = (
    spark.read.table(GOLD_TABLE)
    .filter(col("stockitem_skey") == DEFAULT_SKEY)
    .count() > 0
)

if not default_exists:
    from pyspark.sql.types import (
        StructType, StructField, IntegerType, StringType,
        TimestampType, BooleanType, DecimalType
    )
    from decimal import Decimal

    default_schema = StructType([
        StructField("stockitem_skey",          IntegerType(), False),
        StructField("StockItemID",             IntegerType(), False),
        StructField("StockItemName",           StringType()),
        StructField("SupplierID",              IntegerType()),
        StructField("ColorID",                 IntegerType()),
        StructField("UnitPackageID",           IntegerType()),
        StructField("Brand",                   StringType()),
        StructField("Size",                    StringType()),
        StructField("TaxRate",                 DecimalType(18, 3)),
        StructField("UnitPrice",               DecimalType(18, 2)),
        StructField("RecommendedRetailPrice",  DecimalType(18, 2)),
        StructField("Barcode",                 StringType()),
        StructField("Tags",                    StringType()),
        StructField("CustomFields",            StringType()),
        StructField("SearchDetails",           StringType()),
        StructField("LastEditedBy",            IntegerType()),
        StructField("scd_from",                TimestampType()),
        StructField("scd_to",                  TimestampType()),
        StructField("scd_version",             IntegerType()),
        StructField("scd_active",              IntegerType()),
        StructField("inferred_flag",           IntegerType()),
        StructField("audit_ts",                TimestampType()),
        StructField("updated_audit_ts",        TimestampType()),
        StructField("source_id",               StringType()),
        StructField("row_hash",                StringType()),
    ])

    default_row = spark.createDataFrame(
        [(
            DEFAULT_SKEY, -1, "n/a",
            -1, -1, -1, "", "",
            Decimal("0.000"), Decimal("0.00"), Decimal("0.00"),
            "", "", "", "",
            -1,
            datetime(1900, 1, 1),
            SCD_TO_INFINITY,
            DEFAULT_VERSION, DEFAULT_ACTIVE, INFERRED_FALSE,
            current_audit_ts,
            None,
            SOURCE_ID,
            "DEFAULT"
        )],
        schema=default_schema
    )

    default_row.write.format("delta").mode("append").saveAsTable(GOLD_TABLE)
    print("Default row (stockitem_skey = -1) inserted")
else:
    print("Default row already exists — no action")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Save watermark + Verify
max_silver_audit_consumed = (
    spark.read.table(SILVER_TABLE)
    .filter(col("audit_ts") > lit(max_gold_audit))
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
default_count = result.filter(col("stockitem_skey") == DEFAULT_SKEY).count()
real_count = result.filter(col("stockitem_skey") != DEFAULT_SKEY).count()

print(f"\n=== Total rows: {total} ===")
print(f"Default row:  {default_count}")
print(f"Real rows:    {real_count}  (should equal source StockItem count)")

display(result.filter(col("stockitem_skey") != DEFAULT_SKEY).limit(5))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
