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

# Read Bronze: Get the lates batch not consumed to Silver layer, just the latest audit_ts in Bronze
# Compare to the current Silver state
#   + BK + same hash -> SKIP, do nothing
#   + BK + different hash -> insert new updated version (append-only not update or delete old rows)
#   + Bronze has this BK, but Silver not -> insert new BK
#   + Silver has this BK, but Bronze not -> mark soft-delete
# Still keep history, avoid duplicate
# dedup silver customers

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import (
    col,                        
    lit,                        # convert Python value → Spark literal column
    sha2,                       
    concat_ws,                  # concate columns with delimiter
    coalesce,                  
    trim,                       
    max as spark_max,           
    row_number,                 
    desc                        
)
from pyspark.sql.window import Window    
from delta.tables import DeltaTable      
from datetime import datetime            

BRONZE_PATH    = "Files/bronze/wwi_customers/"
SILVER_TABLE   = "silver.wwi_customers"
BUSINESS_KEY   = "CustomerID"
WATERMARK_FLOW = "bronze_wwi_customers_TO_silver_wwi_customers"

BUSINESS_COLS = [
    "CustomerName", "BillToCustomerID", "CustomerCategoryID",
    "PrimaryContactPersonID", "DeliveryCityID", "PostalCityID",
    "CreditLimit", "AccountOpenedDate", "PhoneNumber", "FaxNumber",
    "WebsiteURL", "DeliveryAddressLine1", "DeliveryAddressLine2",
    "IsOnCreditHold", "LastEditedBy",
]

# (key + business + meta)
ALL_SILVER_COLS = [BUSINESS_KEY] + BUSINESS_COLS + [
    "audit_ts",          # when rows are inserted to Silver
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

# - Silver INSERT-only so max_silver_audit 
# MAX(audit_ts) = is the adjacent/nearest previous run of data ingestion from Bronze to Silver
# or else simplier just when last time Silver consumed Bronze
# - Fallback '1900-01-01' if Silver is empty (for 1st run)
# - Purpose for: lower bound filter 

# silver_df = all history rows (total) existed in silver table, many versions per business key
# max_audit_row["m"]      # → datetime(2026, 6, 18, 14, 0, 0)
# max_audit_row.m         # → datetime(2026, 6, 18, 14, 0, 0)
silver_df = spark.read.table(SILVER_TABLE)
max_audit_row = silver_df.agg(spark_max("audit_ts").alias("m")).first() # find the MAX(audit_ts) across ALL Silver rows, return as a 1-row object

if max_audit_row["m"] is None:
    max_silver_audit = "1900-01-01 00:00:00" # the audit_ts of the most recent Silver INSERT batch
else:
    # Convert datetime → string for using filter lit()
    # max_silver_audit = the nearest previous timestamp Silver run
    max_silver_audit = str(max_audit_row["m"]) 
    print(f"Silver has consumed Bronze up to: {max_silver_audit}")
    print(f"Total history rows: {silver_df.count()}")
    print(f"\nRows from last Silver run (audit_ts = {max_silver_audit}):")
    display(silver_df.filter(col("audit_ts") == lit(max_silver_audit))) # lit(), to convert Python value into Column



# current_audit_ts 
# = unified timestamp for current Silver batch (get one/batch)
# Every row inserted in this notebook will have the same timestamp batch
# Answer for: "which  Silver batch load this row"
# when THIS silver batch runs
current_audit_ts = datetime.now()
print(f"Current Silver batch audit_ts (this run): {current_audit_ts}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# tmp_silver — latest state Silver, latest version per key 
# When comparing with Bronze to detect CHANGED, we must compare Bronze against the CURRENT STATE of Silver — i.e. the latest version of each customer, not an old version.

# Silver INSERT-only will have many row per CustomerID (history).
# get the latest ROW per CustomerID:

# rules, blueprint like partition by business_key and des(audit_ts)
# WindowSpec 
window_silver = Window.partitionBy(BUSINESS_KEY).orderBy(desc("audit_ts"))

# tmp_silver total rows with full columns (latest version per business key)
# current_state per-businesskey latest, because they do not uniformed timestamp
# tmp_silver represents the CURRENT STATE of Silver
tmp_silver = (
    silver_df
    .filter(col("deleted_audit_ts").isNull())
    .withColumn("rn", row_number().over(window_silver))   # mark rank number for each row
    .filter(col("rn") == 1)                                # keep the latest version
    .drop("rn")                                            # drop helper column
)

print(f"Silver state per business key (latest version): {tmp_silver.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# BUILD tmp_bronze — read all Bronze parquet + cast + row_hash
# Full load means each Bronze run = full snapshot. 
# The latest snapshot already contains the complete current state of source. No need to read old snapshots (they're Bronze history, not needed for Silver comparison).
# → Different from incremental: incremental reads ALL batches > marker (because each batch is a delta, must combine them).
# read all parquet (recursive into YYYY/MM/DD/) ─
# recursiveFileLookup=true để Spark recursive into sub-folders
bronze_df = spark.read.option("recursiveFileLookup", "true").parquet(BRONZE_PATH)


# find the latest batch in Bronze, latest snapshot in Bronze
# max_bronze_audit = latest timestamp batch ingested in Bronze
# Full load: we just need batch latest (1 file = full snapshot source)
# in etl.watermark -> to know when silver run, consumed Bronze up to which batch
# MAX(audit_ts) across all Bronze parquet files = timestamp of the latest Bronze batch existed
max_bronze_audit_row = bronze_df.agg(spark_max("audit_ts").alias("m")).first()
max_bronze_audit = max_bronze_audit_row["m"]
print(f"Max Bronze audit_ts (latest batch): {max_bronze_audit}")


# filter Latest Batch only 
# max_bronze_audit: the latest batch ingested in Bronze (full snapshot)
tmp_bronze = bronze_df.filter(
    (col("audit_ts") > lit(max_silver_audit)) &
    (col("audit_ts") == lit(max_bronze_audit))
)


# cast types to Silver schema
tmp_bronze = (
    tmp_bronze
    .withColumn("CustomerID",             col("CustomerID").cast("int"))
    .withColumn("CustomerName",           col("CustomerName").cast("string"))
    .withColumn("BillToCustomerID",       col("BillToCustomerID").cast("int"))
    .withColumn("CustomerCategoryID",     col("CustomerCategoryID").cast("int"))
    .withColumn("PrimaryContactPersonID", col("PrimaryContactPersonID").cast("int"))
    .withColumn("DeliveryCityID",         col("DeliveryCityID").cast("int"))
    .withColumn("PostalCityID",           col("PostalCityID").cast("int"))
    .withColumn("CreditLimit",            col("CreditLimit").cast("decimal(18,2)"))
    .withColumn("AccountOpenedDate",      col("AccountOpenedDate").cast("date"))
    .withColumn("PhoneNumber",            col("PhoneNumber").cast("string"))
    .withColumn("FaxNumber",              col("FaxNumber").cast("string"))
    .withColumn("WebsiteURL",             col("WebsiteURL").cast("string"))
    .withColumn("DeliveryAddressLine1",   col("DeliveryAddressLine1").cast("string"))
    .withColumn("DeliveryAddressLine2",   col("DeliveryAddressLine2").cast("string"))
    .withColumn("IsOnCreditHold",         col("IsOnCreditHold").cast("boolean"))
    .withColumn("LastEditedBy",           col("LastEditedBy").cast("int"))
)


# compute row_hash for detect change
# - '^' (caret) sentinel: distinguished NULL with empty string ''
# - '|' delimiter: avoid "AB"+"C" collision with "A"+"BC"
# - string cast to uniformed data type
# → 64-char hex string per row (256-bit hash)
# Convert a column value into a safe string for hashing — distinguishing NULL from empty/whitespace.
# Simply, NULL "^" and empty string will be ""
def safe_str(c):
    return coalesce(trim(col(c).cast("string")), lit("^"))

# Compute row_hash: concat all BUSINESS_COLS with '|', and after that SHA256
tmp_bronze = tmp_bronze.withColumn(
    "row_hash",
    sha2(
        concat_ws("|", *[safe_str(c) for c in BUSINESS_COLS]),
        256   # 256-bit → 64 character hex
    )
)

print(f"Latest Bronze batch (after filter + hash): {tmp_bronze.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# INSERT CHANGED — existed BK + different row hash

# - INNER JOIN: intersection
# - Filter different row_hash => updated
# - Select columns are inserted from Bronze
# - solve n business key.


changed_df = (
    # Latest Batch only from Bronze layer
    tmp_bronze.alias("brz")
    .join(
        # tmp_silver total rows with full columns (latest version per business key)
        tmp_silver.alias("slv"),
        on=col(f"brz.{BUSINESS_KEY}") == col(f"slv.{BUSINESS_KEY}"),
        how="inner"   
    )
    .filter(col("brz.row_hash") != col("slv.row_hash"))   
    .select(
        col(f"brz.{BUSINESS_KEY}"),                        
        *[col(f"brz.{c}") for c in BUSINESS_COLS],         
        col("brz.row_hash"),                               
        col("brz.source_id")                               
    )
)


# add meta cols audit_ts + deleted_audit_ts
# - audit_ts = batch time của Silver (constant for every rows/batch)
# - deleted_audit_ts = NULL (new row, not deleted yet)
# .select(*ALL_SILVER_COLS) reorder columns corresponding to Silver schema
changed_to_insert = (
    changed_df
    .withColumn("audit_ts", lit(current_audit_ts).cast("timestamp"))
    .withColumn("deleted_audit_ts", lit(None).cast("timestamp"))
    .select(*ALL_SILVER_COLS)   # reorder column order match DDL
)


# INSERT into Silver Delta
# - .mode("append"): INSERT-only, no UPDATE old-version
# - keep old row reserved in Silver (history)
updated_count = changed_to_insert.count()

if updated_count > 0:
    print("Preview rows to be inserted: ")
    display(changed_to_insert)
    changed_to_insert.write.format("delta").mode("append").saveAsTable(SILVER_TABLE)

print(f"Inserted CHANGED versions: {updated_count} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Find new keys Silver havent' had yet
# - LEFT ANTI JOIN: keys in Bronze but it hasn't had in Silver
new_df = (
    # Latest Batch only from Bronze layer
    tmp_bronze.alias("brz")
    .join(
        tmp_silver.alias("slv").select(BUSINESS_KEY),   # tmp_silver total rows with full columns (latest version per business key)
        on=BUSINESS_KEY,
        how="left_anti"   # left anti = "in left, NOT in right"
    )
)


# add meta cols + reorder
new_to_insert = (
    new_df
    .withColumn("audit_ts", lit(current_audit_ts).cast("timestamp")) #  fill value into column
    .withColumn("deleted_audit_ts", lit(None).cast("timestamp"))
    .select(*ALL_SILVER_COLS)
)


# INSERT
new_count = new_to_insert.count()

if new_count > 0:
    display(new_to_insert)
    new_to_insert.write.format("delta").mode("append").saveAsTable(SILVER_TABLE)

print(f"Inserted NEW keys: {new_count} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# SOFT-DELETE find Silver keys that no longer exist in source (Bronze latest batch)
# mark them as soft-deleted by setting deleted_audit_ts on their latest version.
# why only full load -> full snapshot of source, if the key hasnt' existed in snapshot -> source has deleted it
# Incremental load = Bronze batch is only delta (recent changes or insert new), missing key just means no recent edit (NOT "DELETED")
# Guard: if tmp_bronze empty skip for avoiding "deleted all from source" → mark deleted whole Silver
is_bronze_empty = tmp_bronze.count() == 0 # no new batch since last run so the Silver business key seem to be "missing" mark deleted in Silver

if not is_bronze_empty:
    # find missing keys 
    # - LEFT ANTI JOIN: (Silver left, Bronze right)
    missing_keys_df = (
        tmp_silver.alias("slv")
        .join(
            tmp_bronze.alias("brz").select(BUSINESS_KEY),
            on=BUSINESS_KEY,
            how="left_anti"
        )
        .filter(col("deleted_audit_ts").isNull())   # Filter only keys not marked soft-delete
        .select(BUSINESS_KEY)
    )
    
    
    # Find latest audit_ts row in Silver layer for each missing key
    # - Each missing key need to be UPDATED exact latest row LATEST (audit_ts max)
    # - NO UPDATE history (old version of that row — just update the rows represent "current-state"
    keys_with_max_audit = (
        silver_df
        .join(missing_keys_df, on=BUSINESS_KEY, how="inner")
        .groupBy(BUSINESS_KEY)
        .agg(spark_max("audit_ts").alias("max_audit"))
    )
    
    deleted_count = keys_with_max_audit.count()
    
    if deleted_count > 0:
        # For each missing key → find latest version row in Silver → set its deleted_audit_ts = current_audit_ts
        # UPDATE deleted_audit_ts using Delta MERGE
        # - MERGE match (key, audit_ts) → UPDATE only latest row
        # - .whenMatchedUpdate: set deleted_audit_ts = current_audit_ts
        delta_silver = DeltaTable.forName(spark, SILVER_TABLE)
        
        delta_silver.alias("tgt").merge(
            keys_with_max_audit.alias("src"),
            f"tgt.{BUSINESS_KEY} = src.{BUSINESS_KEY} AND tgt.audit_ts = src.max_audit"
        ).whenMatchedUpdate(
            # Silver acknowedge that this key was deleted out of source when Silver batch run
            set = {"deleted_audit_ts": lit(current_audit_ts).cast("timestamp")} 
        ).execute()
        
        print(f"Soft-deleted {deleted_count} keys")
    else:
        print("No keys to soft-delete")
else:
    print("tmp_bronze empty — skip soft-delete")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# SAVE WATERMARK 
# - timestamp: current_timestamp()
# - object_name: WATERMARK_FLOW
# - watermark_value: timestamp of latest Bronze batch consumed in Silver this run
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

# VERIFY
#   - Total rows (comprise history through many runs)
#   - Latest active keys (RANK=1, not mark soft-delete) 
#   - Sample 5 rows

# Check Silver after insert
silver_after = spark.read.table(SILVER_TABLE)


# Total rows 
print(f"=== Total rows in Silver (history): {silver_after.count()} ===")


# Latest active keys
# RANK=1 + filter deleted_audit_ts IS NULL
window_check = Window.partitionBy(BUSINESS_KEY).orderBy(desc("audit_ts"))

latest_active = (
    silver_after
    .withColumn("rn", row_number().over(window_check))
    .filter((col("rn") == 1) & col("deleted_audit_ts").isNull())
)
print(f"=== Latest active keys: {latest_active.count()} ===")


# Sample 5 rows
print("\n=== Sample 5 rows ===")
display(silver_after.limit(5))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
