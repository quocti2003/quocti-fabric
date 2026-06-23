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
    col,                        
    lit,                        
    sha2,                      
    concat_ws,                  
    coalesce,                   
    trim,                       
    max as spark_max,           
    row_number,                 
    desc                        
)
from pyspark.sql.window import Window    
from delta.tables import DeltaTable      
from datetime import datetime            

BRONZE_PATH    = "Files/bronze/wwi_stockitems/"
SILVER_TABLE   = "silver.wwi_stockitems"
BUSINESS_KEY   = "StockItemID"
WATERMARK_FLOW = "bronze_wwi_stockitems_TO_silver_wwi_stockitems"

BUSINESS_COLS = [
    "StockItemName", "SupplierID", "ColorID", "UnitPackageID",
    "Brand", "Size", "TaxRate", "UnitPrice", "RecommendedRetailPrice",
    "Barcode", "Tags", "CustomFields", "SearchDetails", "LastEditedBy"
]

# ALL_SILVER_COLS
# Full Silver column list in DDL order (key + business + meta)
# Use .select(*ALL_SILVER_COLS) to ensure column order matches Delta schema on append
ALL_SILVER_COLS = [BUSINESS_KEY] + BUSINESS_COLS + [
    "audit_ts",          # when row was INSERTed into Silver
    "deleted_audit_ts",  # NULL if active, set if soft-deleted
    "source_id",         # 'WWI' (carried from Bronze)
    "row_hash"           # SHA256 for change detection
]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# GET MARKER — When did Silver last consume from Bronze?

# max_silver_audit
# - Silver is INSERT-only so MAX(audit_ts) = the last Silver batch
# - Fallback '1900-01-01' if Silver is empty (first run)
# - Used as lower-bound filter for Bronze in tmp_bronze

# silver_df: stockitems silver table
silver_df = spark.read.table(SILVER_TABLE)

max_audit_row = silver_df.agg(
    spark_max("audit_ts").alias("max_silver_audit_ts")
).first()

# max_silver_audit = the nearest previous timestamp Silver run
#                  = last time Silver consumed Bronze
#                  = MAX(audit_ts) of silver table
if max_audit_row["max_silver_audit_ts"] is None:
    # Silver empty → fallback (every Bronze row passes filter > '1900-01-01')
    max_silver_audit = "1900-01-01 00:00:00"
else:
    # Convert datetime → string for use in filter lit()
    max_silver_audit = str(max_audit_row["max_silver_audit_ts"])

print(f"Max Silver audit_ts: {max_silver_audit}")

# current_audit_ts
# = current Silver batch time (taken once, used consistently)
# - Every row INSERTed in this notebook gets this same value
# - answer question "which Silver batch loaded this row"
current_audit_ts = datetime.now()
print(f"Current Silver batch audit_ts: {current_audit_ts}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# BUILD tmp_silver — latest Silver state per key

# Silver is INSERT-only so multiple rows can exist per StockItemID (history).
# We need the LATEST row per StockItemID.

# Window definition
# - PARTITION BY StockItemID: group rows sharing the same business key
# - ORDER BY audit_ts DESC: latest row first within each group
latest_per_key_window = Window.partitionBy(BUSINESS_KEY).orderBy(desc("audit_ts"))


# Apply window + filter version_rank=1
# - row_number() assigns 1, 2, 3 within each group (1 = latest)
# - Keep version_rank=1 = latest row per key
# - Drop helper after use
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

# tmp_bronze — read Bronze + cast + row_hash

# read all parquet (recurse into YYYY/MM/DD/)
# recursiveFileLookup=true makes Spark walk sub-folders
bronze_df = spark.read.option("recursiveFileLookup", "true").parquet(BRONZE_PATH)


# find latest Bronze batch
# - max_bronze_audit = time of the most recent Bronze Copy run
# - Full load: only need the latest batch (1 file = full source snapshot)
# max_bronze_audit = MAX(audit_ts) across all Bronze parquet
max_bronze_audit_row = bronze_df.agg(
    spark_max("audit_ts").alias("max_bronze_audit_ts")
).first()
max_bronze_audit = max_bronze_audit_row["max_bronze_audit_ts"]
print(f"Max Bronze audit_ts (latest batch): {max_bronze_audit}")


# filter Latest Batch only + not yet consumed
# - audit_ts > max_silver_audit: batches newer than last Silver load
# - audit_ts == max_bronze_audit: keep only the latest Bronze batch (full snapshot)
# - Picks exactly 1 batch (newest, not yet consumed)
tmp_bronze = bronze_df.filter(
    (col("audit_ts") > lit(max_silver_audit)) &
    (col("audit_ts") == lit(max_bronze_audit))
)


# cast types to Silver schema
# Bronze parquet types may be mis-inferred on read (e.g. UnitPrice as STRING)
# Cast explicitly to match Silver DDL
tmp_bronze = (
    tmp_bronze
    .withColumn("StockItemID",            col("StockItemID").cast("int"))
    .withColumn("StockItemName",          col("StockItemName").cast("string"))
    .withColumn("SupplierID",             col("SupplierID").cast("int"))
    .withColumn("ColorID",                col("ColorID").cast("int"))
    .withColumn("UnitPackageID",          col("UnitPackageID").cast("int"))
    .withColumn("Brand",                  col("Brand").cast("string"))
    .withColumn("Size",                   col("Size").cast("string"))
    .withColumn("TaxRate",                col("TaxRate").cast("decimal(18,3)"))
    .withColumn("UnitPrice",              col("UnitPrice").cast("decimal(18,2)"))
    .withColumn("RecommendedRetailPrice", col("RecommendedRetailPrice").cast("decimal(18,2)"))
    .withColumn("Barcode",                col("Barcode").cast("string"))
    .withColumn("Tags",                   col("Tags").cast("string"))
    .withColumn("CustomFields",           col("CustomFields").cast("string"))
    .withColumn("SearchDetails",          col("SearchDetails").cast("string"))
    .withColumn("LastEditedBy",           col("LastEditedBy").cast("int"))
)



# compute row_hash
# - '^' (caret) sentinel: distinguishes NULL from empty string ''
# - '|' delimiter: avoids "AB"+"C" colliding with "A"+"BC"
# - Cast to string for consistent hashing across types
# → 64-char hex string per row (256-bit hash)
def safe_str(column_name):
    return coalesce(trim(col(column_name).cast("string")), lit("^"))

tmp_bronze = tmp_bronze.withColumn(
    "row_hash",
    sha2(
        concat_ws("|", *[safe_str(c) for c in BUSINESS_COLS]),
        256   # 256-bit → 64 hex chars
    )
)

print(f"Source from Bronze (tmp_bronze): {tmp_bronze.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# INSERT CHANGED — row_hash differs → INSERT new version

# find rows where row_hash differs
# - INNER JOIN: intersection, keys in both bronze and silver
# - Filter different row_hash -> data has changed/updated
# - Select INSERT columns (from Bronze, not Silver)
changed_df = (
    tmp_bronze.alias("bronze")
    .join(
        tmp_silver.alias("silver"),
        on=col(f"bronze.{BUSINESS_KEY}") == col(f"silver.{BUSINESS_KEY}"),
        how="inner"   # keep only rows on both sides
    )
    .filter(col("bronze.row_hash") != col("silver.row_hash"))   # row changed
    .select(
        col(f"bronze.{BUSINESS_KEY}"),                          # StockItemID from Bronze
        *[col(f"bronze.{c}") for c in BUSINESS_COLS],           # business cols from Bronze
        col("bronze.row_hash"),                                 # new hash
        col("bronze.source_id")                                 # 'WWI' carried from Bronze
    )
)


# add meta cols audit_ts + deleted_audit_ts
# - audit_ts = Silver batch time (fixed for every row in this batch)
# - deleted_audit_ts = NULL (new row, not deleted)
# .select(*ALL_SILVER_COLS) reorders columns to match Silver schema
changed_to_insert = (
    changed_df
    .withColumn("audit_ts", lit(current_audit_ts).cast("timestamp"))
    .withColumn("deleted_audit_ts", lit(None).cast("timestamp"))
    .select(*ALL_SILVER_COLS)
)


# INSERT into Silver Delta
# - .mode("append"): INSERT-only, no UPDATE of old rows
# - Old Silver rows remain untouched (history)
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

# find new keys (not yet in Silver)
# - LEFT ANTI JOIN: keys in Bronze BUT NOT in Silver (Bronze to the left, Silver to the right)
new_df = (
    tmp_bronze.alias("bronze")
    .join(
        tmp_silver.alias("silver").select(BUSINESS_KEY),   # only the key is needed
        on=BUSINESS_KEY,
        how="left_anti"   # left anti = "in left, NOT in right"
    )
)


# add meta cols + reorder
new_to_insert = (
    new_df
    .withColumn("audit_ts", lit(current_audit_ts).cast("timestamp"))
    .withColumn("deleted_audit_ts", lit(None).cast("timestamp"))
    .select(*ALL_SILVER_COLS)
)


# INSERT
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

# SOFT-DELETE missing keys
# Only applies to full load:
#   - Full load can see "which key disappeared from source" → mark deleted

# Guard: if tmp_bronze is empty (batch did not match), skip — otherwise we'd mistakenly think "source dropped everything" and soft-delete all Silver
is_bronze_empty = tmp_bronze.count() == 0

if not is_bronze_empty:
    # find missing keys
    # - LEFT ANTI JOIN: keys in Silver (tmp_silver) but NOT in Bronze (tmp_bronze)
    # - Filter only keys NOT yet soft-deleted
    missing_keys_df = (
        tmp_silver.alias("silver")
        .join(
            tmp_bronze.alias("bronze").select(BUSINESS_KEY),
            on=BUSINESS_KEY,
            how="left_anti"
        )
        .filter(col("deleted_audit_ts").isNull())   # skip keys already soft-deleted
        .select(BUSINESS_KEY)
    )


    # find audit_ts of the LATEST row in Silver
    # - Each missing key needs to UPDATE its LATEST row (max audit_ts)
    # - NOT UPDATE history (older rows) — only the row representing "current state"
    keys_with_latest_audit = (
        silver_df
        .join(missing_keys_df, on=BUSINESS_KEY, how="inner")
        .groupBy(BUSINESS_KEY)
        .agg(spark_max("audit_ts").alias("latest_audit_ts"))
    )

    deleted_count = keys_with_latest_audit.count()

    if deleted_count > 0:
        # UPDATE deleted_audit_ts via Delta MERGE
        # - MERGE matches exact (key, audit_ts) → updates only the latest row
        # - .whenMatchedUpdate: set deleted_audit_ts = current_audit_ts
        silver_delta = DeltaTable.forName(spark, SILVER_TABLE)

        silver_delta.alias("silver_target").merge(
            keys_with_latest_audit.alias("delete_source"),
            f"silver_target.{BUSINESS_KEY} = delete_source.{BUSINESS_KEY} "
            f"AND silver_target.audit_ts = delete_source.latest_audit_ts"
        ).whenMatchedUpdate(
            set = {"deleted_audit_ts": lit(current_audit_ts).cast("timestamp")}
        ).execute()

        print(f"Soft-deleted {deleted_count} keys")
    else:
        print("No keys to soft-delete")
else:
    print("tmp_bronze empty — skip soft-delete (safety)")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# SAVE WATERMARK — log to etl.watermark

# Build a 1-row DataFrame
# - timestamp: now() (log time)
# - object_name:WATERMARK_WORKFLOW
# - watermark_value: max Bronze audit_ts consumed (tmp_bronze)
watermark_data = [(
    datetime.now(),
    WATERMARK_FLOW,
    str(max_bronze_audit)
)]

watermark_df = spark.createDataFrame(
    watermark_data,
    ["timestamp", "object_name", "watermark_value"]
)


# INSERT into etl.watermark (append-only)
watermark_df.write.format("delta").mode("append").saveAsTable("etl.watermark")
print(f"Watermark saved: {WATERMARK_FLOW} = {max_bronze_audit}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# VERIFY — count rows + sample
# Confirm Silver has correct data:
#   - Total rows (including history across runs)
#   - Latest active keys (RANK=1, not soft-deleted) 
#   - Sample 5 rows

# Re-read Silver after INSERT
silver_after = spark.read.table(SILVER_TABLE)


# Total rows
print(f"=== Total rows in Silver (history): {silver_after.count()} ===")


# Latest active keys
# Apply version_rank=1 + filter deleted_audit_ts IS NULL
verify_window = Window.partitionBy(BUSINESS_KEY).orderBy(desc("audit_ts"))

latest_active = (
    silver_after
    .withColumn("version_rank", row_number().over(verify_window))
    .filter((col("version_rank") == 1) & col("deleted_audit_ts").isNull())
)
print(f"=== Latest active keys: {latest_active.count()} ===")


# Sample 5 rows for visual check
print("\n=== Sample 5 rows ===")
display(silver_after.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
