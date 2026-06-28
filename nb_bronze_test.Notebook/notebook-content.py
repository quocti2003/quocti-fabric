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

import shutil
import os
from pyspark.sql.utils import AnalysisException

# ============================================================
# 1. Drop Delta tables (Silver + Gold + etl.watermark)
# ============================================================
tables_to_drop = [
    # Gold
    "gold.fact_invoiceline",
    "gold.dim_customer",
    "gold.dim_stockitem",
    "gold.dim_date",
    
    # Silver
    "silver.wwi_customers",
    "silver.wwi_stockitems",
    "silver.wwi_invoices",
    "silver.wwi_invoicelines",
    
    # ETL control (will be recreated by nb_setup_control_table)
    "etl.watermark",
    "etl.pipeline_metadata"
]

for tbl in tables_to_drop:
    try:
        spark.sql(f"DROP TABLE IF EXISTS {tbl}")
        print(f"✅ Dropped {tbl}")
    except AnalysisException as e:
        print(f"⚠️ {tbl}: {e}")

# ============================================================
# 2. Clear Bronze parquet files
# ============================================================
bronze_root = "/lakehouse/default/Files/bronze"
if os.path.exists(bronze_root):
    for folder in os.listdir(bronze_root):
        full_path = os.path.join(bronze_root, folder)
        if os.path.isdir(full_path):
            shutil.rmtree(full_path)
            print(f"✅ Cleared {full_path}")
else:
    print("Bronze folder doesn't exist (fresh state)")

# ============================================================
# 3. Verify all cleared
# ============================================================
print("\n=== Verify clean state ===")
for schema in ["etl", "silver", "gold"]:
    try:
        tables = spark.sql(f"SHOW TABLES IN {schema}").collect()
        if not tables:
            print(f"  {schema}: empty ✅")
        else:
            for t in tables:
                print(f"  {schema}.{t['tableName']} STILL EXISTS ❌")
    except AnalysisException:
        print(f"  {schema}: schema dropped or not exist")

if os.path.exists(bronze_root):
    contents = os.listdir(bronze_root)
    print(f"  Bronze files: {len(contents)} items {'(should be 0)' if contents else '✅'}")
else:
    print("  Bronze: folder removed ✅")

print("\n=== Clean complete — Ready to re-initialize ===")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for tbl, expected in [
    ("wwi_customers", 663),
    ("wwi_stockitems", 227),
    ("wwi_invoices", 70510),
    ("wwi_invoicelines", 228265),
]:
    df = spark.read.option("recursiveFileLookup", "true").parquet(f"Files/bronze/{tbl}/")
    actual = df.count()
    status = "✅" if actual >= expected else "❌"
    print(f"{status} {tbl}: {actual} rows (expected ≥ {expected})")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ═════════════════════════════════════════════════════════════
# S2 — BASELINE — Capture state BEFORE UPDATE
# Run this cell first, note the output.
# Then: go to Azure SQL Query editor → UPDATE → run pipeline → run Cell 3.
# ═════════════════════════════════════════════════════════════

print(f"=== S2 BASELINE (before UPDATE) ===\n")

bronze_c = read_bronze(BRONZE_CUSTOMERS)
max_audit, row_count = latest_batch_info(bronze_c)

print(f"Latest batch audit_ts: {max_audit}")
print(f"Latest batch row count: {row_count}")

print(f"\nCustomerID={TEST_CUSTOMER_ID} history in Bronze:")
display(
    bronze_c
    .filter(col("CustomerID") == TEST_CUSTOMER_ID)
    .select("CustomerID", "CustomerName", "CreditLimit", "audit_ts")
    .orderBy("audit_ts")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ═════════════════════════════════════════════════════════════
# S2 — VERIFY — Check Bronze captured the UPDATE
# Pre-requisite:
#   1. Cell 2 baseline captured
#   2. Azure SQL: UPDATE Sales.Customers SET CreditLimit = 12345.67 WHERE CustomerID = 1
#   3. pl_bronze_ingest run successfully
#
# Expected:
#   - New batch with row_count = 663 (full snapshot)
#   - Customer 1 in latest batch has CreditLimit = 12345.67
#   - Customer 1 history shows old + new value
# ═════════════════════════════════════════════════════════════

print(f"=== S2 AFTER (UPDATE + pipeline) ===\n")

bronze_c = read_bronze(BRONZE_CUSTOMERS)
max_audit, row_count = latest_batch_info(bronze_c)

print(f"Latest batch audit_ts: {max_audit}   ← should be NEWER than baseline")
print(f"Latest batch row count: {row_count}  ← Expected: 663 (full snapshot)")

print(f"\nCustomerID={TEST_CUSTOMER_ID} history in Bronze:")
display(
    bronze_c
    .filter(col("CustomerID") == TEST_CUSTOMER_ID)
    .select("CustomerID", "CustomerName", "CreditLimit", "audit_ts")
    .orderBy("audit_ts")
)

print(f"\nCustomerID={TEST_CUSTOMER_ID} in latest batch only:")
display(
    bronze_c
    .filter(col("audit_ts") == max_audit)
    .filter(col("CustomerID") == TEST_CUSTOMER_ID)
    .select("CustomerID", "CustomerName", "CreditLimit", "audit_ts")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ═════════════════════════════════════════════════════════════
# S1 — BASELINE — capture state BEFORE INSERT new Customer 9999
# ═════════════════════════════════════════════════════════════

print(f"=== S1 BASELINE ===\n")

bronze_c = read_bronze(BRONZE_CUSTOMERS)
max_audit, row_count = latest_batch_info(bronze_c)

print(f"Latest batch audit_ts: {max_audit}")
print(f"Latest batch row count: {row_count}   ← Expected: 663")

# Check NEW_CUSTOMER_ID NOT exists yet
print(f"\nCustomerID={NEW_CUSTOMER_ID} should NOT exist yet:")
display(
    bronze_c
    .filter(col("CustomerID") == NEW_CUSTOMER_ID)
    .select("CustomerID", "CustomerName", "CreditLimit", "audit_ts")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ═════════════════════════════════════════════════════════════
# S1 — VERIFY — after INSERT + pipeline
# Expected:
#   - Latest batch row count = 664 (= 663 + 1 new)
#   - CustomerID=9999 appears in latest batch only (not in old batches)
# ═════════════════════════════════════════════════════════════

print(f"=== S1 AFTER (INSERT + pipeline) ===\n")

bronze_c = read_bronze(BRONZE_CUSTOMERS)
max_audit, row_count = latest_batch_info(bronze_c)

print(f"Latest batch audit_ts: {max_audit}")
print(f"Latest batch row count: {row_count}   ← Expected: 664")

print(f"\nCustomerID={NEW_CUSTOMER_ID} across all Bronze batches:")
display(
    bronze_c
    .filter(col("CustomerID") == NEW_CUSTOMER_ID)
    .select("CustomerID", "CustomerName", "CreditLimit", "audit_ts")
    .orderBy("audit_ts")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("=" * 70)
print(" PHASE 1 — TEST CANDIDATES")
print("=" * 70)

# Customer candidate for DELETE (0 invoices = safe to delete)
print("\n=== Customer candidates for DELETE (0 invoices) ===")
display(spark.sql("""
    SELECT c.CustomerID, c.CustomerName, COUNT(i.InvoiceID) AS n_invoices
    FROM silver.wwi_customers c
    LEFT JOIN silver.wwi_invoices i 
        ON c.CustomerID = i.CustomerID AND i.deleted_audit_ts IS NULL
    WHERE c.deleted_audit_ts IS NULL
    GROUP BY c.CustomerID, c.CustomerName
    HAVING COUNT(i.InvoiceID) = 0
    ORDER BY c.CustomerID
    LIMIT 3
"""))

# Customer candidates for UPDATE
print("\n=== Customer candidates for UPDATE ===")
display(spark.sql("""
    SELECT CustomerID, CustomerName, CreditLimit, PhoneNumber
    FROM silver.wwi_customers
    WHERE deleted_audit_ts IS NULL
    ORDER BY CustomerID
    LIMIT 3
"""))

# StockItem candidate for DELETE (0 lines)
print("\n=== StockItem candidates for DELETE (0 lines) ===")
display(spark.sql("""
    SELECT s.StockItemID, s.StockItemName, COUNT(l.InvoiceLineID) AS n_lines
    FROM silver.wwi_stockitems s
    LEFT JOIN silver.wwi_invoicelines l 
        ON s.StockItemID = l.StockItemID AND l.deleted_audit_ts IS NULL
    WHERE s.deleted_audit_ts IS NULL
    GROUP BY s.StockItemID, s.StockItemName
    HAVING COUNT(l.InvoiceLineID) = 0
    ORDER BY s.StockItemID
    LIMIT 3
"""))

# StockItem candidates for UPDATE
print("\n=== StockItem candidates for UPDATE ===")
display(spark.sql("""
    SELECT StockItemID, StockItemName, UnitPrice, TaxRate
    FROM silver.wwi_stockitems
    WHERE deleted_audit_ts IS NULL
    ORDER BY StockItemID
    LIMIT 3
"""))

# Invoice candidate for UPDATE — pick có ít lines để dễ verify
print("\n=== Invoice candidates for UPDATE (with line count) ===")
display(spark.sql("""
    SELECT i.InvoiceID, i.CustomerID, i.InvoiceDate, COUNT(l.InvoiceLineID) AS n_lines
    FROM silver.wwi_invoices i
    LEFT JOIN silver.wwi_invoicelines l 
        ON i.InvoiceID = l.InvoiceID AND l.deleted_audit_ts IS NULL
    WHERE i.deleted_audit_ts IS NULL
    GROUP BY i.InvoiceID, i.CustomerID, i.InvoiceDate
    ORDER BY n_lines ASC, i.InvoiceID
    LIMIT 3
"""))

# InvoiceLine candidates for UPDATE — pick lines của Invoice sẽ update để dễ verify fact propagation
print("\n=== InvoiceLine candidates for UPDATE ===")
display(spark.sql("""
    SELECT InvoiceLineID, InvoiceID, StockItemID, Quantity, UnitPrice
    FROM silver.wwi_invoicelines
    WHERE deleted_audit_ts IS NULL
    ORDER BY InvoiceLineID
    LIMIT 3
"""))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("=" * 70)
print(" PHASE 1 — BASELINE STATE")
print("=" * 70)

# Bronze
print("\n=== BRONZE ===")
for tbl in ["wwi_customers", "wwi_stockitems", "wwi_invoices", "wwi_invoicelines"]:
    df = spark.read.option("recursiveFileLookup","true").parquet(f"Files/bronze/{tbl}/")
    n_batches = df.select("audit_ts").distinct().count()
    print(f"  {tbl}: {df.count():,} rows / {n_batches} batches")

# Silver
print("\n=== SILVER (active / total) ===")
for tbl, key in [
    ("silver.wwi_customers", "CustomerID"),
    ("silver.wwi_stockitems", "StockItemID"),
    ("silver.wwi_invoices", "InvoiceID"),
    ("silver.wwi_invoicelines", "InvoiceLineID"),
]:
    total = spark.sql(f"SELECT COUNT(*) FROM {tbl}").first()[0]
    active = spark.sql(f"SELECT COUNT(DISTINCT {key}) FROM {tbl} WHERE deleted_audit_ts IS NULL").first()[0]
    print(f"  {tbl}: {active:,} active / {total:,} total")

# Gold
print("\n=== GOLD ===")
for tbl in ["gold.dim_customer", "gold.dim_stockitem", "gold.dim_date", "gold.fact_invoiceline"]:
    cnt = spark.sql(f"SELECT COUNT(*) FROM {tbl}").first()[0]
    print(f"  {tbl}: {cnt:,}")

# SCD2 detail
print("\n=== dim_customer SCD2 breakdown ===")
display(spark.sql("""
    SELECT 
        COUNT(*) AS total_rows,
        SUM(CASE WHEN scd_active = 1 THEN 1 ELSE 0 END) AS active_versions,
        SUM(CASE WHEN scd_active = 0 THEN 1 ELSE 0 END) AS expired_versions,
        MAX(scd_version) AS max_scd_version
    FROM gold.dim_customer
"""))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
