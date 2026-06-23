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
from decimal import Decimal


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
SCD_VERSION_FIX = 1            # SCD1: always 1
SCD_ACTIVE_FIX  = 1            # SCD1: always 1
INFERRED_FALSE  = 0
SOURCE_ID       = "WWI"
SCD_TO_INFINITY = datetime(9999, 12, 31)

print(f"Loading: {SILVER_TABLE} → {GOLD_TABLE}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# GET MARKER

gold_df = spark.read.table(GOLD_TABLE)

# Marker — SCD1 also tracks audit_ts on initial INSERT (updated_audit_ts on changes)
# For "has Silver changed since last load?" use max(audit_ts) of dim rows.
max_audit_row = gold_df.agg(
    spark_max("audit_ts").alias("max_gold_audit_ts")
).first()

if max_audit_row["max_gold_audit_ts"] is None:
    max_gold_audit = "1900-01-01 00:00:00"
else:
    max_gold_audit = str(max_audit_row["max_gold_audit_ts"])

print(f"Max Gold audit_ts: {max_gold_audit}")


current_audit_ts = datetime.now()
print(f"Current Gold batch audit_ts: {current_audit_ts}")


# Max existing skey (excludes default -1)
max_skey_row = (
    gold_df.filter(col("stockitem_skey") != DEFAULT_SKEY)
    .agg(spark_max("stockitem_skey").alias("max_skey"))
    .first()
)

max_existing_skey = max_skey_row["max_skey"] if max_skey_row["max_skey"] is not None else 0
print(f"Max existing stockitem_skey: {max_existing_skey} (new skeys will start at {max_existing_skey + 1})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# BUILD tmp_dim — existing rows (excluding default)
# SCD1 has only 1 row per StockItemID (no history) — no need to
# filter scd_active. Just exclude default row.

tmp_dim = gold_df.filter(col(BUSINESS_KEY) != DEFAULT_SKEY) \
                 .filter(col("stockitem_skey") != DEFAULT_SKEY)

print(f"Existing dim rows (tmp_dim): {tmp_dim.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# BUILD tmp_silver — latest active per StockItemID since marker for filtering Silver (rows newer than last Gold load). 
# Same pattern as dim_customer Cell 4:
#   - Filter audit_ts > marker + not deleted
#   - RANK = 1 per StockItemID (latest)
#   - Compute row_hash (SHA256 with '^' sentinel + '|' delim)

silver_df = spark.read.table(SILVER_TABLE)

silver_recent = (
    silver_df
    .filter(col("audit_ts") > lit(max_gold_audit))
    .filter(col("deleted_audit_ts").isNull())
)

latest_window = Window.partitionBy(BUSINESS_KEY).orderBy(desc("audit_ts"))

tmp_silver = (
    silver_recent
    .withColumn("version_rank", row_number().over(latest_window))
    .filter(col("version_rank") == 1)
    .filter(col("deleted_audit_ts").isNull())
    .drop("version_rank")
)

# row_hash
def safe_str(column_name):
    return coalesce(trim(col(column_name).cast("string")), lit("^"))

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

# UPDATE CHANGED — SCD1 in-place overwrite via Delta MERGE
# In Delta: MERGE WHEN MATCHED AND src.row_hash <> tgt.row_hash THEN UPDATE.

# Build dict of columns to update on row_hash mismatch
# Dict mapping each business col to its source value:
update_set = {c: f"src.{c}" for c in BUSINESS_COLS}
update_set["row_hash"]         = "src.row_hash"
update_set["updated_audit_ts"] = "cast('{ts}' as timestamp)".format(ts=current_audit_ts)

# Use Delta MERGE: match on natural key, update only if row_hash differs
DeltaTable.forName(spark, GOLD_TABLE).alias("tgt").merge(
    tmp_silver.alias("src"),
    f"tgt.{BUSINESS_KEY} = src.{BUSINESS_KEY}"
).whenMatchedUpdate(
    condition = "src.row_hash != tgt.row_hash",
    set       = update_set
).execute()

# Count how many were updated (re-query, since merge doesn't return count directly)
# Approximation: count dim rows where updated_audit_ts = current_audit_ts
updated_count = (
    spark.read.table(GOLD_TABLE)
    .filter(col("updated_audit_ts") == lit(current_audit_ts).cast("timestamp"))
    .count()
)
print(f"UPDATED in-place (row_hash differs): {updated_count} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# INSERT NEW — StockItemID not yet in dim
# Same pattern as dim_customer Cell 6: LEFT ANTI JOIN + allocate skeys.
# Differences:
#   - scd_version, scd_active fixed at 1 (SCD1)
#   - updated_audit_ts = NULL on first INSERT (only set on UPDATE)

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

new_count = new_rows.count()
print(f"NEW rows to insert: {new_count}")

if new_count > 0:
    # Allocate skeys
    skey_window = Window.orderBy(BUSINESS_KEY)

    new_with_skey = (
        new_rows
        .withColumn("stockitem_skey",
                    (lit(max_existing_skey) + row_number().over(skey_window)).cast("int"))

        # SCD1 fixed values
        .withColumn("scd_from",         lit(current_audit_ts).cast("timestamp"))
        .withColumn("scd_to",           lit(SCD_TO_INFINITY).cast("timestamp"))
        .withColumn("scd_version",      lit(SCD_VERSION_FIX).cast("int"))
        .withColumn("scd_active",       lit(SCD_ACTIVE_FIX).cast("int"))
        .withColumn("inferred_flag",    lit(INFERRED_FALSE).cast("int"))

        # Audit
        .withColumn("audit_ts",         lit(current_audit_ts).cast("timestamp"))
        .withColumn("updated_audit_ts", lit(None).cast("timestamp"))    # NULL on first INSERT
        .withColumn("source_id",        lit(SOURCE_ID))
    )

    ALL_GOLD_COLS = [
        "stockitem_skey", BUSINESS_KEY, *BUSINESS_COLS,
        "scd_from", "scd_to", "scd_version", "scd_active", "inferred_flag",
        "audit_ts", "updated_audit_ts", "source_id", "row_hash"
    ]

    new_with_skey.select(*ALL_GOLD_COLS) \
        .write.format("delta").mode("append").saveAsTable(GOLD_TABLE)

    print(f"Inserted {new_count} new rows (skeys {max_existing_skey + 1} → {max_existing_skey + new_count})")
else:
    print("No new StockItemIDs to insert")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ENSURE DEFAULT ROW (stockitem_skey = -1)
# Idempotent — only inserts if not exists.

default_exists = (
    spark.read.table(GOLD_TABLE)
    .filter(col("stockitem_skey") == DEFAULT_SKEY)
    .count() > 0
)

if not default_exists:
    from pyspark.sql.types import (
        StructType, StructField, IntegerType, StringType,
        TimestampType, DecimalType
    )

    default_schema = StructType([
        StructField("stockitem_skey",         IntegerType(), False),
        StructField("StockItemID",            IntegerType(), False),
        StructField("StockItemName",          StringType()),
        StructField("SupplierID",             IntegerType()),
        StructField("ColorID",                IntegerType()),
        StructField("UnitPackageID",          IntegerType()),
        StructField("Brand",                  StringType()),
        StructField("Size",                   StringType()),
        StructField("TaxRate",                DecimalType(18, 3)),
        StructField("UnitPrice",              DecimalType(18, 2)),
        StructField("RecommendedRetailPrice", DecimalType(18, 2)),
        StructField("Barcode",                StringType()),
        StructField("Tags",                   StringType()),
        StructField("CustomFields",           StringType()),
        StructField("SearchDetails",          StringType()),
        StructField("LastEditedBy",           IntegerType()),
        StructField("scd_from",               TimestampType()),
        StructField("scd_to",                 TimestampType()),
        StructField("scd_version",            IntegerType()),
        StructField("scd_active",             IntegerType()),
        StructField("inferred_flag",          IntegerType()),
        StructField("audit_ts",               TimestampType()),
        StructField("updated_audit_ts",       TimestampType()),
        StructField("source_id",              StringType()),
        StructField("row_hash",               StringType()),
    ])

    default_row = spark.createDataFrame(
        [(
            DEFAULT_SKEY,                # stockitem_skey
            -1,                          # StockItemID
            "n/a",                       # StockItemName
            -1, -1, -1,                  # SupplierID, ColorID, UnitPackageID
            "", "",                      # Brand, Size
            Decimal("0.000"),            # TaxRate
            Decimal("0.00"),             # UnitPrice
            Decimal("0.00"),             # RecommendedRetailPrice
            "", "", "", "",              # Barcode, Tags, CustomFields, SearchDetails
            -1,                          # LastEditedBy
            datetime(1900, 1, 1),        # scd_from
            SCD_TO_INFINITY,             # scd_to
            SCD_VERSION_FIX,             # scd_version
            SCD_ACTIVE_FIX,              # scd_active
            INFERRED_FALSE,              # inferred_flag
            current_audit_ts,            # audit_ts
            None,                        # updated_audit_ts (NULL — default row never updated)
            SOURCE_ID,                   # source_id
            "DEFAULT"                    # row_hash sentinel
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

#  SAVE WATERMARK + VERIFY

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

total          = result.count()
default_count  = result.filter(col("stockitem_skey") == DEFAULT_SKEY).count()
real_count     = result.filter(col("stockitem_skey") != DEFAULT_SKEY).count()

print(f"\n=== Total rows: {total} ===")
print(f"Default row:    {default_count}")
print(f"Real stockitems: {real_count} (should equal unique StockItemIDs in Silver = ~227)")

print("\n=== Sample 5 stockitems ===")
display(
    result.filter(col("stockitem_skey") != DEFAULT_SKEY)
          .select("stockitem_skey", "StockItemID", "StockItemName", "Brand",
                  "UnitPrice", "scd_version", "scd_active",
                  "audit_ts", "updated_audit_ts")
          .orderBy("stockitem_skey")
          .limit(5)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
