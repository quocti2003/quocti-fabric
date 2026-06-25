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
DEFAULT_SKEY    = -1 # Sentinel for unresolved fact FKs
DEFAULT_VERSION = 1 # initial scd version (recomputed later)
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

gold_before_insert = spark.read.table(GOLD_TABLE) # read full Gold table 

max_gold_audit = (
    gold_before_insert.agg(spark_max("audit_ts").alias("m")).first()["m"]
)

# max_gold_audit: last time Gold consumed Silver
if max_gold_audit is None:
    max_gold_audit = "1900-01-01 00:00:00"   # fallback for first run
else:
    max_gold_audit = str(max_gold_audit)

print(f"Max Gold audit_ts (marker): {max_gold_audit}")

# max_existing_skey — highest skey used (excluding -1 default)
max_existing_skey = (
    gold_before_insert
    .filter(col("customer_skey") != DEFAULT_SKEY)
    .agg(spark_max("customer_skey").alias("m")).first()["m"]
)
if max_existing_skey is None:
    max_existing_skey = 0
print(f"Max existing customer_skey: {max_existing_skey} (new skeys start at {max_existing_skey + 1})")

# tmp_dim — just take all current ACTIVE version per CustomerID in dim (for change detection)
tmp_dim = (
    gold_before_insert
    .filter(col("scd_active") == 1)
    .filter(col("customer_skey") != DEFAULT_SKEY)
)

# print(f"Current active dim versions (tmp_dim): {tmp_dim.count()} rows")
# display(tmp_dim)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_df = spark.read.table(SILVER_TABLE)

# filter to rows newer than marker (no deleted filter here — rank first)
silver_recent = silver_df.filter(col("audit_ts") > lit(max_gold_audit))

# rank ALL versions (including deleted) — pick absolute latest per CustomerID
latest_window = Window.partitionBy(BUSINESS_KEY).orderBy(desc("audit_ts"))

tmp_silver = (
    silver_recent
    .withColumn("version_rank", row_number().over(latest_window))
    .filter(col("version_rank") == 1)
    .filter(col("deleted_audit_ts").isNull())          
    .drop("version_rank")
)

# compute row_hash over business cols 
def safe_str(column_name):
    return coalesce(trim(col(column_name).cast("string")), lit("^"))

# tmp_silver = at most 1 row per CustomerID (latest version per business key, with row_hash)
tmp_silver = tmp_silver.withColumn(
    "row_hash",
    sha2(concat_ws("|", *[safe_str(c) for c in BUSINESS_COLS]), 256)
)

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

# Stamp batch + Allocate skeys + INSERT
# - Skey strategy: max_existing_skey + row_number() OVER (orderBy BUSINESS_KEY)
# - SCD2 placeholder values (recomputed later)

current_audit_ts = datetime.now()
print(f"Current batch audit_ts: {current_audit_ts}")


if total_to_insert > 0:
    # Allocate skeys sequentially from max_existing_skey + 1
    skey_window = Window.orderBy(BUSINESS_KEY)

    rows_with_skey = (
        rows_to_insert
        .withColumn("customer_skey",
                    (lit(max_existing_skey) + row_number().over(skey_window)).cast("int"))
        # SCD2 initial values (recomputed in Phase 5)
        .withColumn("scd_from",      lit(current_audit_ts).cast("timestamp"))
        .withColumn("scd_to",        lit(SCD_TO_INFINITY).cast("timestamp"))
        .withColumn("scd_version",   lit(DEFAULT_VERSION).cast("int"))   # placeholder
        .withColumn("scd_active",    lit(DEFAULT_ACTIVE).cast("int"))    # placeholder
        .withColumn("inferred_flag", lit(INFERRED_FALSE).cast("int"))
        # Audit / lineage
        .withColumn("audit_ts",      lit(current_audit_ts).cast("timestamp"))
        .withColumn("source_id",     lit(SOURCE_ID))
    )

    # Column order matching Gold DDL exactly
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
            lag("audit_ts").over(timeline_window).isNull(),     # version 1 (no LAG) → 1900-01-01
            lit(datetime(1900, 1, 1)).cast("timestamp")          
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
        StructField("customer_skey",          IntegerType(), False), # NOT NULL
        StructField("CustomerID",             IntegerType(), False), # NOT NULL
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
            -1, -1, -1, -1, -1,           # 5 IDs BillToCustomerID, CustomerCategoryID, PrimaryContactPersonID, DeliveryCityID, PostalCityID
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

# When fact uses default row
# - CustomerID NULL in source
# - Business key not in dim (late-arriving)

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
