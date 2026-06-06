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

for schema in ['bronze', 'silver', 'gold', 'etl']:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    print(f"Created schema: {schema}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
CREATE TABLE etl.pipeline_metadata (
    source_schema     STRING,
    source_table      STRING,
    target_table      STRING,
    load_type         STRING,
    watermark_column  STRING,
    business_key      STRING,
    custom_query      STRING,
    is_active         BOOLEAN
) USING DELTA
""")
print("Created etl.pipeline_metadata")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
INSERT INTO etl.pipeline_metadata VALUES

-- Full: Customers
('Sales', 'Customers', 'wwi_customers', 'full', NULL, 'CustomerID',
 'SELECT CustomerID, CustomerName, BillToCustomerID, CustomerCategoryID, PrimaryContactPersonID, DeliveryCityID, PostalCityID, CreditLimit, AccountOpenedDate, PhoneNumber, FaxNumber, WebsiteURL, DeliveryAddressLine1, DeliveryAddressLine2, IsOnCreditHold, LastEditedBy FROM Sales.Customers',
 true),

-- Full: StockItems
('Warehouse', 'StockItems', 'wwi_stockitems', 'full', NULL, 'StockItemID',
 'SELECT StockItemID, StockItemName, SupplierID, ColorID, UnitPackageID, Brand, Size, TaxRate, UnitPrice, RecommendedRetailPrice, Barcode, Tags, CustomFields, SearchDetails, LastEditedBy FROM Warehouse.StockItems',
 true),

-- Incremental: Invoices (custom_query no WHERE query)
('Sales', 'Invoices', 'wwi_invoices', 'incremental', 'LastEditedWhen', 'InvoiceID',
 'SELECT * FROM Sales.Invoices',
 true),

-- Incremental: InvoiceLines
('Sales', 'InvoiceLines', 'wwi_invoicelines', 'incremental', 'LastEditedWhen', 'InvoiceLineID',
 'SELECT * FROM Sales.InvoiceLines',
 true)
""")

display(spark.sql("SELECT source_table, load_type, custom_query FROM etl.pipeline_metadata"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
CREATE TABLE etl.watermark (
    timestamp        TIMESTAMP,
    object_name      STRING,
    watermark_value  STRING
) USING DELTA
""")
print("Created etl.watermark (empty)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
