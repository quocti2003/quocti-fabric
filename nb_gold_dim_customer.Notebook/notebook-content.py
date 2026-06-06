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

# [1] Setup workspace  
#   Declare what tables I'm reading from and writing to. 
#   List the columns I care about. Define some constants like "infinity = 9999-12-31."
# [2] "Where did I leave off last time?"
#   Look at Gold and find: when did I last load? (max_gold_audit). What's the highest skey I've already used? (max_existing_skey). 
#   Also: what time is it right now? (current_audit_ts) — I'll stamp every new row with this timestamp.
# [3] "What does Gold currently know?" 
#   Build tmp_dim — current active versions in Gold (scd_active=1) 
#   Read Gold, keep only the CURRENT version of each customer (scd_active = 1). This is my "before" snapshot — what's already in there.
# [4] "What's new from Silver?" - Build tmp_silver — latest active per CustomerID since marker + row_hash
#   Read Silver, but ONLY rows newer than my last load. For each CustomerID, keep just the latest version. 
#   Compute a fingerprint (row_hash) — a SHA-256 of all business columns — so I can detect changes quickly.
# [5] "What's actually different?" - Detect CHANGED (row_hash differs) and NEW (CustomerID not in dim)
#   Compare Silver (new) vs Gold (current):
#       Customer exists in both, fingerprint differs → CHANGED (their data updated).
#       Customer in Silver but not in Gold → NEW (first-time customer).
#       Both go into one pile: rows_to_insert.
# [6] "Assign keys and write them in." Allocate new skeys + INSERT new versions 
#   Generate new surrogate keys (start at max_existing_skey + 1, count up). 
#   Stamp every row with the batch timestamp, source_id = "WWI", and placeholder SCD2 values. 
#   Append into Gold.
# [7] Recompute SCD2 timeline (LAG/LEAD → UPDATE scd_from/to/version/active)
# [8] "Make sure my -1 safety net exists." Ensure default row (customer_skey = -1) exists
# [9] "Log this run and check my work." Save watermark + Verify

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import (
    col, lit, sha2, concat_ws, coalesce, trim,
    max as spark_max, min as spark_min,
    row_number, lead, lag, desc, when, current_timestamp,
    monotonically_increasing_id
)
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, TimestampType
from delta.tables import DeltaTable
from datetime import datetime


SILVER_TABLE   = "silver.wwi_customers"
GOLD_TABLE     = "gold.dim_customer"
BUSINESS_KEY   = "CustomerID"
WATERMARK_FLOW = "silver_wwi_customers_TO_gold_dim_customer"

# Business cols — used for row_hash + column selection on INSERT
# Mirrors silver.wwi_customers business columns (NOT audit/meta).
BUSINESS_COLS = [
    "CustomerName", "BillToCustomerID", "CustomerCategoryID",
    "PrimaryContactPersonID", "DeliveryCityID", "PostalCityID",
    "CreditLimit", "AccountOpenedDate", "PhoneNumber", "FaxNumber",
    "WebsiteURL", "DeliveryAddressLine1", "DeliveryAddressLine2",
    "IsOnCreditHold", "LastEditedBy"
]

# Default-row sentinels (customer_skey = -1 for unknown)
DEFAULT_SKEY    = -1 # to index and link dimension records to fact tables
DEFAULT_VERSION = 1
DEFAULT_ACTIVE  = 1
INFERRED_FALSE  = 0
SOURCE_ID       = "WWI"

# Far-future sentinel for scd_to of current versions
SCD_TO_INFINITY = datetime(9999, 12, 31)

print(f"Loading: {SILVER_TABLE} → {GOLD_TABLE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# This cell code do 3 things
# 1/ max_gold_audit: last time this flow consumed Silver run succefully
# 2/ current_audit_ts: So all rows from one uniformed batch share one timestamp
# 3/ max_existing_skey: highest skey already used (excluding -1 default)
#   → new skeys start at max_existing_skey + 1

# GOLD_TABLE     = "gold.dim_customer" 
gold_df = spark.read.table(GOLD_TABLE) # reads ALL of gold.dim_customer, -1 default row also

# Marker: MAX(audit_ts) from Gold dim (fallback if first run)
max_audit_row = gold_df.agg(
    spark_max("audit_ts").alias("max_gold_audit_ts")
).first()

# max_gold_audit: the timestamp of the most recent batch this Silver -> gold (dim_customer) notebook successfully wrote
if max_audit_row["max_gold_audit_ts"] is None:
    max_gold_audit = "1900-01-01 00:00:00"
else:
    max_gold_audit = str(max_audit_row["max_gold_audit_ts"])

print(f"The timestamp of the most recent batch this Silver -> gold (dim_customer): {max_gold_audit}")


# Current batch time (frozen for this run)
current_audit_ts = datetime.now()
print(f"Current Gold batch audit_ts: {current_audit_ts}")


# Max existing skey (excludes default row -1)
max_skey_row = (
    gold_df.filter(col("customer_skey") != DEFAULT_SKEY)
    .agg(spark_max("customer_skey").alias("max_skey"))
    .first()
)
# output of max_skey_row is Row object (not DF)
# we can access the value 2 ways 
# max_skey_row["max_skey"]      → 663
# max_skey_row.max_skey         → 663

#  "What's the highest customer_skey already used in Gold?"
# max_existing_skey A Python variable just in memory during one notebook run, holds the MAX of customer_skey
max_existing_skey = max_skey_row["max_skey"] if max_skey_row["max_skey"] is not None else 0
# new skeys start at max_existing_skey + 1
print(f"Max existing customer_skey: {max_existing_skey} (new skeys will start at {max_existing_skey + 1})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# tmp_dim — current active version per CustomerID
# - Filter scd_active = 1 (current version only), because scd_active = 0 is historical
# - Exclude default row (customer_skey = -1)
# - Used to compare row_hash and detect CHANGED rows
# - It takes a snapshot of each CustomerID (business_key) in Gold looks like right now (representing the current version only).
# - gold_df all rows from gold dim table 
# - SCD2 dims hold every historical version, in other words, 1 row can have many version (like updated/changed data)
# - Why we need this tmp_dim ?
# => we just only care about the active version (the latest one).
tmp_dim = (
    gold_df
    .filter(col("scd_active") == 1)
    .filter(col("customer_skey") != DEFAULT_SKEY)
)

print(f"Current active dim versions (tmp_dim): {tmp_dim.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# tmp_silver — latest active per CustomerID since marker
# - Filter audit_ts > max_gold_audit (only new/changed Silver rows)
# - For each CustomerID, keep latest version (RANK = 1 over audit_ts DESC)
# - Drop deleted (deleted_audit_ts IS NULL)
# - Compute row_hash (SHA256 with '^' sentinel + '|' delimiter)

silver_df = spark.read.table(SILVER_TABLE)

# filter to rows newer than marker AND not deleted
silver_recent = (
    silver_df
    .filter(col("audit_ts") > lit(max_gold_audit))
    .filter(col("deleted_audit_ts").isNull())
)

# keep latest per CustomerID
latest_window = Window.partitionBy(BUSINESS_KEY).orderBy(desc("audit_ts"))

tmp_silver = (
    silver_recent
    .withColumn("version_rank", row_number().over(latest_window))
    .filter(col("version_rank") == 1)
    .drop("version_rank")
)

# compute row_hash over business cols 
def safe_str(column_name):
    return coalesce(trim(col(column_name).cast("string")), lit("^"))

tmp_silver = tmp_silver.withColumn(
    "row_hash",
    sha2(concat_ws("|", *[safe_str(c) for c in BUSINESS_COLS]), 256)
)

# Finally, tmp_silver on row per CustomerID, no two rows share the same business key
# Conclusion, tmp_silver
#   1/ at most one row per CustomerID
#   2/ Each row = the LATEST Silver version of that CustomerID (for this table)
#   3/ but need newer than audit_ts the timestamp of the most recent batch this Silver -> gold (dim_customer) notebook successfully wrote
#   4/ that row not soft-deleted too
#   5/ row-hash too
print(f"Latest Silver per CustomerID (tmp_silver): {tmp_silver.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# DETECT CHANGED + NEW
# - CHANGED: CustomerID exists in dim, but row_hash differs → INSERT new version
# - NEW: CustomerID not in dim → INSERT first version
# Both go into rows_to_insert (combined for skey allocation).

# CHANGED rows
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

# NEW rows, because the result has ONLY silver columns so we dont need to silver. like above
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

# Combine for unified skey allocation because 
# result sets have the same schema -> one combined DataFrame of everything that needs to be addeed to Gold
# Notice we don't distinguish CHANGED vs NEW any further. From Gold's perspective both are just "rows to insert as new SCD versions."
# unionByName -> stacks 2 DF vertical into one -> just simply understand that they stacking two tables on top each other -> matching columns by name (order doesn't matter)
rows_to_insert = changed_rows.unionByName(new_rows)

changed_count = changed_rows.count()
new_count = new_rows.count()
total_to_insert = changed_count + new_count

print(f"CHANGED rows: {changed_count}")
print(f"NEW rows: {new_count}")
print(f"Total to INSERT: {total_to_insert}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ALLOCATE SKEYS + INSERT new versions
# - Skey strategy: max_existing_skey + row_number() OVER (orderBy CustomerID)
#   Delta has no IDENTITY, so we generate skey at load time.
# - Set initial SCD2 values: scd_from=current_audit_ts, scd_to=9999-12-31,
#   scd_active=1, scd_version will be recomputed

if total_to_insert > 0:
    # Allocate skeys sequentially starting from max_existing_skey + 1
    skey_window = Window.orderBy(BUSINESS_KEY)



    rows_with_skey = (
        rows_to_insert
        .withColumn("customer_skey",
                    (lit(max_existing_skey) + row_number().over(skey_window)).cast("int"))

        # SCD2 initial values (timeline recomputed in Cell 7)
        .withColumn("scd_from",      lit(current_audit_ts).cast("timestamp"))
        .withColumn("scd_to",        lit(SCD_TO_INFINITY).cast("timestamp"))
        .withColumn("scd_version",   lit(1).cast("int"))     # placeholder, recomputed later
        .withColumn("scd_active",    lit(1).cast("int"))     # placeholder, recomputed later
        .withColumn("inferred_flag", lit(INFERRED_FALSE).cast("int"))

        # Audit / lineage
        .withColumn("audit_ts",      lit(current_audit_ts).cast("timestamp"))
        .withColumn("source_id",     lit(SOURCE_ID))
    )

    # Final column order — must match DDL exactly
    ALL_GOLD_COLS = [
        "customer_skey", BUSINESS_KEY, *BUSINESS_COLS,
        "scd_from", "scd_to", "scd_version", "scd_active", "inferred_flag",
        "audit_ts", "source_id", "row_hash"
    ]

    rows_with_skey.select(*ALL_GOLD_COLS) \
        .write.format("delta").mode("append").saveAsTable(GOLD_TABLE)

    print(f"Inserted {total_to_insert} rows (skeys {max_existing_skey + 1} → {max_existing_skey + total_to_insert})")
else:
    print("No CHANGED or NEW rows — nothing to insert")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# RECOMPUTE SCD2 TIMELINE
# For each CustomerID, recompute:
#   - scd_version = row_number() ORDER BY audit_ts ASC
#   - scd_to = LEAD(audit_ts) OR 9999-12-31 N's "until" is version N+1's start. latest version has no next -> 9999-12-31 
#   - scd_active = 1 if no next version, else 0 (latest version is active)
# Use LAG/LEAD windows.
#
# Implementation: read non-default rows, recompute, MERGE back by customer_skey.

non_default = (
    spark.read.table(GOLD_TABLE)
    .filter(col("customer_skey") != DEFAULT_SKEY)
)

timeline_window = Window.partitionBy(BUSINESS_KEY).orderBy("audit_ts")

recomputed = (
    non_default
    .withColumn("new_scd_version", row_number().over(timeline_window).cast("int"))
    .withColumn(
        "new_scd_to",
        coalesce(
            lead("audit_ts").over(timeline_window),
            lit(SCD_TO_INFINITY).cast("timestamp")
        )
    )
    .withColumn(
        "new_scd_active",
        when(lead("audit_ts").over(timeline_window).isNull(), 1).otherwise(0).cast("int")
    )
    .withColumn(
        "new_scd_from",
        when(
            lag("audit_ts").over(timeline_window).isNull(),     # version 1
            lit(datetime(1900, 1, 1)).cast("timestamp")          # → 1900-01-01
        ).otherwise(col("audit_ts"))                             # version N>1: own audit_ts
    )
    .select(
        "customer_skey",
        col("new_scd_from").alias("scd_from"),
        col("new_scd_to").alias("scd_to"),
        col("new_scd_version").alias("scd_version"),
        col("new_scd_active").alias("scd_active"),
    )
)

# MERGE recomputed timeline back into gold table
# In the cell above, the rows already existed in Gold. we are not adding/removing - we're updating column in place
# Delta MERGE on customer_skey matches each Gold row to its recomputed values and updates just the 4 SCD2 columns.
DeltaTable.forName(spark, GOLD_TABLE).alias("tgt").merge(
    recomputed.alias("src"),
    "tgt.customer_skey = src.customer_skey"
).whenMatchedUpdate(set={
    "scd_from":    "src.scd_from",
    "scd_to":      "src.scd_to",
    "scd_version": "src.scd_version",
    "scd_active":  "src.scd_active",
}).execute()

print(f"SCD2 timeline recomputed for {recomputed.count()} dim rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ENSURE DEFAULT ROW (customer_skey = -1)
# Idempotent — only inserts if -1 row doesn't exist yet.
# Fact rows with unknown/NULL CustomerID will point to this skey.
# handle fact rows cant resolve a dimension key
# Invoice references CustomerID = 5000, but dim_customer doesn't have 5000 (sync delay, bad data)
# CustomerID is NULL in source
# Point-in-time SCD2 condition doesn't match any version
# when this happens, fact needs default customer_skey to point to => that is the default row (-1) safe target
default_exists = (
    spark.read.table(GOLD_TABLE)
    .filter(col("customer_skey") == DEFAULT_SKEY)
    .count() > 0
)

if not default_exists:
    from pyspark.sql.types import (
        StructType, StructField, IntegerType, StringType,
        DateType, TimestampType, BooleanType, DecimalType
    )
    from decimal import Decimal

    default_schema = StructType([
        StructField("customer_skey",          IntegerType(), False),
        StructField("CustomerID",             IntegerType(), False),
        StructField("CustomerName",           StringType()),
        StructField("BillToCustomerID",       IntegerType()),
        StructField("CustomerCategoryID",     IntegerType()),
        StructField("PrimaryContactPersonID", IntegerType()),
        StructField("DeliveryCityID",         IntegerType()),
        StructField("PostalCityID",           IntegerType()),
        StructField("CreditLimit",            DecimalType(18, 2)),
        StructField("AccountOpenedDate",      DateType()),
        StructField("PhoneNumber",            StringType()),
        StructField("FaxNumber",              StringType()),
        StructField("WebsiteURL",             StringType()),
        StructField("DeliveryAddressLine1",   StringType()),
        StructField("DeliveryAddressLine2",   StringType()),
        StructField("IsOnCreditHold",         BooleanType()),
        StructField("LastEditedBy",           IntegerType()),
        StructField("scd_from",               TimestampType()),
        StructField("scd_to",                 TimestampType()),
        StructField("scd_version",            IntegerType()),
        StructField("scd_active",             IntegerType()),
        StructField("inferred_flag",          IntegerType()),
        StructField("audit_ts",               TimestampType()),
        StructField("source_id",              StringType()),
        StructField("row_hash",               StringType()),
    ])

    default_row = spark.createDataFrame(
        [(
            DEFAULT_SKEY,                 # customer_skey = -1
            -1,                           # CustomerID = -1 (sentinel)
            "n/a",                        # CustomerName
            -1, -1, -1, -1, -1,           # 5 IDs
            Decimal("0.00"),              # CreditLimit
            datetime(2999, 12, 31).date(),# AccountOpenedDate
            "", "", "", "", "",           # phone, fax, url, addr1, addr2
            False,                        # IsOnCreditHold
            -1,                           # LastEditedBy
            datetime(1900, 1, 1),         # scd_from
            SCD_TO_INFINITY,              # scd_to
            1,                            # scd_version
            1,                            # scd_active
            INFERRED_FALSE,               # inferred_flag
            current_audit_ts,             # audit_ts
            SOURCE_ID,                    # source_id
            "DEFAULT"                     # row_hash sentinel
        )],
        schema=default_schema
    )

    default_row.write.format("delta").mode("append").saveAsTable(GOLD_TABLE)
    print("Default row (customer_skey = -1) inserted")
else:
    print("Default row already exists — no action")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# SAVE WATERMARK + VERIFY
# - Append etl.watermark with max Silver audit_ts consumed
# - Guard: only write watermark if there were new/changed Silver rows
#   (consistent with dim_stockitem + fact_invoiceline pattern)

# Compute max Silver audit_ts that this run consumed (for watermark log)
max_silver_audit_consumed = (
    spark.read.table(SILVER_TABLE)
    .filter(col("audit_ts") > lit(max_gold_audit))
    .agg(spark_max("audit_ts").alias("m"))
    .first()["m"]
)

# Save watermark only if Silver had new rows to consume
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
default_count = result.filter(col("customer_skey") == DEFAULT_SKEY).count()
active_count  = result.filter((col("customer_skey") != DEFAULT_SKEY) & (col("scd_active") == 1)).count()
history_count = result.filter((col("customer_skey") != DEFAULT_SKEY) & (col("scd_active") == 0)).count()

print(f"\n=== Total rows: {total} ===")
print(f"Default row:         {default_count}")
print(f"Active versions:     {active_count}   (should equal unique CustomerIDs in Silver)")
print(f"Historical versions: {history_count}  (0 on first run)")

print("\n=== Sample 5 active customers ===")
display(
    result.filter((col("customer_skey") != DEFAULT_SKEY) & (col("scd_active") == 1))
          .select("customer_skey", "CustomerID", "CustomerName", "CustomerCategoryID",
                  "CreditLimit", "scd_from", "scd_to", "scd_version", "scd_active")
          .orderBy("customer_skey")
          .limit(5)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
