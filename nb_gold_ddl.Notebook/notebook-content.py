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

# CREATE gold.dim_date

from delta.tables import DeltaTable
from pyspark.sql.types import IntegerType, DateType, StringType

(
    DeltaTable.createIfNotExists(spark)
        .tableName("gold.dim_date")
        .addColumn("DateKey",                 IntegerType(), nullable=False) # 20260522 INT format YYYMMDD - PK
        .addColumn("Date",                    DateType(),    nullable=False) # 2026-05-22 - type DATE riel
        .addColumn("DateString",              StringType(),  nullable=False) # "2026-05-22"	- text version
        .addColumn("Day",                     IntegerType())                 # 22 - day in a month
        .addColumn("DaySuffix",               StringType())                  # "nd" - (st/nd/rd/th)
        .addColumn("Weekday",                 IntegerType())                 # 6 - day of week (1=Sun, 6=Friday)
        .addColumn("WeekDayName",             StringType())                  # "Friday"	- full name day of week
        .addColumn("WeekDayName_Short",       StringType())                  # "Fri"	- shortcut name day of week (3 characters)
        .addColumn("WeekDayName_FirstLetter", StringType())                  # "F" - first character day of week
        .addColumn("DOWInMonth",              IntegerType())                 # 4 - "Friday 4th in May"
        .addColumn("DayOfYear",               IntegerType())                 # 142 - day 142 in a year
        .addColumn("WeekOfMonth",             IntegerType())                 # which order of week in a month
        .addColumn("WeekOfYear",              IntegerType())                 # which order of week in a year
        .addColumn("Month",                   IntegerType())                 # 4 - month (integer type)
        .addColumn("MonthName",               StringType())                  # "May" - full name of month
        .addColumn("MonthName_Short",         StringType())                  # "May" - shortcut name of month
        .addColumn("MonthName_FirstLetter",   StringType())                  # "M" - first character month name
        .addColumn("Quarter",                 IntegerType())                  
        .addColumn("QuarterName",             StringType())                  # "Q2" - name of quarter
        .addColumn("Year",                    IntegerType())                 # 2026
        .addColumn("YearMonthNo",             IntegerType())                 # 202605 - INT format YYYYMM - sort month through year
        .addColumn("YearMonthName",           StringType())                  # "2026-May" - month name + year display
        .addColumn("IsWeekend",               IntegerType())                 # 1 is weekend, 0 is not
        .execute()
)

print("gold.dim_date created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CREATE gold.dim_customer (SCD Type 2)
# Purpose: customer dimension with full history tracking.
# - SCD2 columns track validity window per version

from delta.tables import DeltaTable
from pyspark.sql.types import (
    IntegerType, StringType, DateType, TimestampType,
    BooleanType, DecimalType
)

(
    DeltaTable.createIfNotExists(spark)
        .tableName("gold.dim_customer")

        # Surrogate key (generated at load time)
        .addColumn("customer_skey",          IntegerType(), nullable=False)

        # Natural key from source
        .addColumn("CustomerID",             IntegerType(), nullable=False)

        # Business columns (mirror silver.wwi_customers)
        .addColumn("CustomerName",           StringType())
        .addColumn("BillToCustomerID",       IntegerType())
        .addColumn("CustomerCategoryID",     IntegerType())
        .addColumn("PrimaryContactPersonID", IntegerType())
        .addColumn("DeliveryCityID",         IntegerType())
        .addColumn("PostalCityID",           IntegerType())
        .addColumn("CreditLimit",            DecimalType(18, 2))
        .addColumn("AccountOpenedDate",      DateType())
        .addColumn("PhoneNumber",            StringType())
        .addColumn("FaxNumber",              StringType())
        .addColumn("WebsiteURL",             StringType())
        .addColumn("DeliveryAddressLine1",   StringType())
        .addColumn("DeliveryAddressLine2",   StringType())
        .addColumn("IsOnCreditHold",         BooleanType())
        .addColumn("LastEditedBy",           IntegerType())

        # SCD2 metadata
        .addColumn("scd_from",               TimestampType())    # start valid
        .addColumn("scd_to",                 TimestampType())    # end valid
        .addColumn("scd_version",            IntegerType())      # 1, 2, 3, ... per CustomerID
        .addColumn("scd_active",             IntegerType())      # 1 = current, 0 = historical
        .addColumn("inferred_flag",          IntegerType())      # 1 = late-arriving placeholder, 0 = real

        # Audit / lineage
        .addColumn("audit_ts",               TimestampType())    # when this version was INSERTed into Gold
        .addColumn("source_id",              StringType())       # 'WWI'
        .addColumn("row_hash",               StringType())       # SHA256 over business cols

        .execute()
)
# (empty, ready for SCD2 data)
print("gold.dim_customer table created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# spark.read.table("gold.dim_customer").printSchema()
# print(f"Row count: {spark.read.table('gold.dim_customer').count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CREATE gold.dim_stockitem (SCD Type 1)
# Purpose: stockitem dim with overwrite-on-change (no history).
# - Keeps SCD2 columns for schema uniformity with other dims
# - scd_version always = 1, scd_active always = 1
# - updated_audit_ts records last UPDATE time

from delta.tables import DeltaTable
from pyspark.sql.types import (
    IntegerType, StringType, TimestampType,
    BooleanType, DecimalType
)

(
    DeltaTable.createIfNotExists(spark)
        .tableName("gold.dim_stockitem")

        # Surrogate + natural keys
        .addColumn("stockitem_skey",         IntegerType(), nullable=False)
        .addColumn("StockItemID",            IntegerType(), nullable=False)

        # Business cols (mirror silver.wwi_stockitems)
        .addColumn("StockItemName",          StringType())
        .addColumn("SupplierID",             IntegerType())
        .addColumn("ColorID",                IntegerType())
        .addColumn("UnitPackageID",          IntegerType())
        .addColumn("Brand",                  StringType())
        .addColumn("Size",                   StringType())
        .addColumn("TaxRate",                DecimalType(18, 3))
        .addColumn("UnitPrice",              DecimalType(18, 2))
        .addColumn("RecommendedRetailPrice", DecimalType(18, 2))
        .addColumn("Barcode",                StringType())
        .addColumn("Tags",                   StringType())
        .addColumn("CustomFields",           StringType())
        .addColumn("SearchDetails",          StringType())
        .addColumn("LastEditedBy",           IntegerType())

        # SCD metadata (kept for uniformity; mostly static for SCD1)
        .addColumn("scd_from",               TimestampType())
        .addColumn("scd_to",                 TimestampType())
        .addColumn("scd_version",            IntegerType())     # always 1 for SCD1
        .addColumn("scd_active",             IntegerType())     # always 1 for SCD1
        .addColumn("inferred_flag",          IntegerType())

        # Audit / lineage
        .addColumn("audit_ts",               TimestampType())   # initial INSERT time
        .addColumn("updated_audit_ts",       TimestampType())   # last UPDATE time (SCD1-specific)
        .addColumn("source_id",              StringType())
        .addColumn("row_hash",               StringType())

        .execute()
)
# (empty, ready for SCD1 data)
print("gold.dim_stockitem table created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.read.table("gold.dim_stockitem").printSchema()
print(f"Row count: {spark.read.table('gold.dim_stockitem').count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CREATE gold.fact_invoiceline
# Purpose: invoice line-grain fact joining customer + stockitem + date dims.
# - invoice_line_skey: surrogate PK (generated at load)
# - Natural keys kept (InvoiceID, InvoiceLineID) for lineage
# - 3 dim FKs (customer_skey, stockitem_skey, invoice_date_key)
# - Header attrs denormalized (InvoiceDate, CustomerPurchaseOrderNumber)
# - Measures: Quantity, UnitPrice, TaxAmount, LineProfit, ExtendedPrice

from delta.tables import DeltaTable
from pyspark.sql.types import (
    IntegerType, LongType, StringType, DateType, TimestampType,
    BooleanType, DecimalType
)

(
    DeltaTable.createIfNotExists(spark)
        .tableName("gold.fact_invoiceline")

        # Surrogate PK
        .addColumn("invoice_line_skey",         LongType(),  nullable=False)

        # Natural keys (lineage back to Silver)
        .addColumn("InvoiceID",                 IntegerType(), nullable=False)
        .addColumn("InvoiceLineID",             IntegerType(), nullable=False)

        # Dim FKs (resolved at load time)
        .addColumn("customer_skey",             IntegerType(), nullable=False)
        .addColumn("stockitem_skey",            IntegerType(), nullable=False)
        .addColumn("invoice_date_key",          IntegerType(), nullable=False)

        # Natural keys of dims (kept for traceability per BizOne pattern)
        .addColumn("CustomerID",                IntegerType())
        .addColumn("StockItemID",               IntegerType())

        # Header attributes (denormalized for query convenience)
        .addColumn("InvoiceDate",               DateType())
        .addColumn("CustomerPurchaseOrderNumber", StringType())
        .addColumn("IsCreditNote",              BooleanType())

        # Detail attributes (line-level descriptive)
        .addColumn("Description",               StringType())
        .addColumn("PackageTypeID",             IntegerType())

        # Measures
        .addColumn("Quantity",                  IntegerType())
        .addColumn("UnitPrice",                 DecimalType(18, 2))
        .addColumn("TaxRate",                   DecimalType(18, 3))
        .addColumn("TaxAmount",                 DecimalType(18, 2))
        .addColumn("LineProfit",                DecimalType(18, 2))
        .addColumn("ExtendedPrice",             DecimalType(18, 2))

        # Audit / lineage
        .addColumn("audit_ts",                  TimestampType())
        .addColumn("source_id",                 StringType())
        .addColumn("row_hash",                  StringType())

        .execute()
)

print("gold.fact_invoiceline table created (empty)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.read.table("gold.fact_invoiceline").printSchema()
print(f"Row count: {spark.read.table('gold.fact_invoiceline').count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
