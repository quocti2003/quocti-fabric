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

# Pre-define schema for Spark enforcing types when INSERT.
# silver.wwi_customers
# - Business cols cast to proper types
# - 4 meta cols: audit_ts, deleted_audit_ts, source_id, row_hash
spark.sql("""
CREATE TABLE silver.wwi_customers (
    CustomerID                INT,
    CustomerName              STRING,
    BillToCustomerID          INT,
    CustomerCategoryID        INT,
    PrimaryContactPersonID    INT,
    DeliveryCityID            INT,
    PostalCityID              INT,
    CreditLimit               DECIMAL(18,2),
    AccountOpenedDate         DATE,
    PhoneNumber               STRING,
    FaxNumber                 STRING,
    WebsiteURL                STRING,
    DeliveryAddressLine1      STRING,
    DeliveryAddressLine2      STRING,
    IsOnCreditHold            BOOLEAN,
    LastEditedBy              INT,
    audit_ts                  TIMESTAMP,
    deleted_audit_ts          TIMESTAMP,
    source_id                 STRING,
    row_hash                  STRING
) USING DELTA
""")
print("Created silver.wwi_customers")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
CREATE TABLE silver.wwi_stockitems (
    StockItemID               INT,
    StockItemName             STRING,
    SupplierID                INT,
    ColorID                   INT,
    UnitPackageID             INT,
    Brand                     STRING,
    Size                      STRING,
    TaxRate                   DECIMAL(18,3),
    UnitPrice                 DECIMAL(18,2),
    RecommendedRetailPrice    DECIMAL(18,2),
    Barcode                   STRING,
    Tags                      STRING,
    CustomFields              STRING,
    SearchDetails             STRING,
    LastEditedBy              INT,
    audit_ts                  TIMESTAMP,
    deleted_audit_ts          TIMESTAMP,
    source_id                 STRING,
    row_hash                  STRING
) USING DELTA
""")
print("Created silver.wwi_stockitems")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
CREATE TABLE silver.wwi_invoices (
    InvoiceID                       INT,
    CustomerID                      INT,
    BillToCustomerID                INT,
    OrderID                         INT,
    DeliveryMethodID                INT,
    ContactPersonID                 INT,
    AccountsPersonID                INT,
    SalespersonPersonID             INT,
    PackedByPersonID                INT,
    InvoiceDate                     DATE,
    CustomerPurchaseOrderNumber     STRING,
    IsCreditNote                    BOOLEAN,
    CreditNoteReason                STRING,
    Comments                        STRING,
    DeliveryInstructions            STRING,
    InternalComments                STRING,
    TotalDryItems                   INT,
    TotalChillerItems               INT,
    DeliveryRun                     STRING,
    RunPosition                     STRING,
    ReturnedDeliveryData            STRING,
    ConfirmedDeliveryTime           TIMESTAMP,
    ConfirmedReceivedBy             STRING,
    LastEditedBy                    INT,
    LastEditedWhen                  TIMESTAMP,
    audit_ts                        TIMESTAMP,
    deleted_audit_ts                TIMESTAMP,
    source_id                       STRING,
    row_hash                        STRING
) USING DELTA
""")
print("Created silver.wwi_invoices")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
CREATE TABLE silver.wwi_invoicelines (
    InvoiceLineID             INT,
    InvoiceID                 INT,
    StockItemID               INT,
    Description               STRING,
    PackageTypeID             INT,
    Quantity                  INT,
    UnitPrice                 DECIMAL(18,2),
    TaxRate                   DECIMAL(18,3),
    TaxAmount                 DECIMAL(18,2),
    LineProfit                DECIMAL(18,2),
    ExtendedPrice             DECIMAL(18,2),
    LastEditedBy              INT,
    LastEditedWhen            TIMESTAMP,
    audit_ts                  TIMESTAMP,
    deleted_audit_ts          TIMESTAMP,
    source_id                 STRING,
    row_hash                  STRING
) USING DELTA
""")
print("Created silver.wwi_invoicelines")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.sql("SHOW TABLES IN silver"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.sql("DESCRIBE silver.wwi_customers"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.sql("DESCRIBE silver.wwi_stockitems"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.sql("DESCRIBE silver.wwi_invoices"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.sql("DESCRIBE silver.wwi_invoicelines"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
