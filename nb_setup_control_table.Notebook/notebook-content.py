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
    create table if not exists etl.watermark (
        timestamp timestamp not null,
        object_name string not null,
        watermark_value string
    )
    using delta
""")
print(f"Created etl.watermark")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("DROP TABLE IF EXISTS etl.pipeline_metadata")
spark.sql("""
    create table if not exists etl.pipeline_metadata (
        source_system string not null,
        source_schema string not null,
        source_table string not null,
        target_schema string not null,
        target_table string not null,
        load_type string not null,
        watermark_column string, 
        dedup_column string,
        business_keys array<string> not null,
        columns_list array<string> not null,
        custom_query string not null,
        is_active boolean not null
    )
    using delta
""")
print(f"Created etl.pipeline_metadata (rows: {spark.read.table('etl.pipeline_metadata').count()})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
INSERT INTO etl.pipeline_metadata VALUES

-- Customers (Sales.Customers, full load)
('WWI', 'Sales', 'Customers', 'bronze', 'wwi_customers', 'full', NULL, NULL,
 ARRAY('CustomerID'),
 ARRAY('CustomerName', 'BillToCustomerID', 'CustomerCategoryID', 'PrimaryContactPersonID',
       'DeliveryCityID', 'PostalCityID', 'CreditLimit', 'AccountOpenedDate',
       'PhoneNumber', 'FaxNumber', 'WebsiteURL', 'DeliveryAddressLine1',
       'DeliveryAddressLine2', 'IsOnCreditHold', 'LastEditedBy'),
 'SELECT CustomerID, CustomerName, BillToCustomerID, CustomerCategoryID, PrimaryContactPersonID, DeliveryCityID, PostalCityID, CreditLimit, AccountOpenedDate, PhoneNumber, FaxNumber, WebsiteURL, DeliveryAddressLine1, DeliveryAddressLine2, IsOnCreditHold, LastEditedBy FROM Sales.Customers',
 true),

-- StockItems (Warehouse.StockItems, full load)
('WWI', 'Warehouse', 'StockItems', 'bronze', 'wwi_stockitems', 'full', NULL, NULL,
 ARRAY('StockItemID'),
 ARRAY('StockItemName', 'SupplierID', 'ColorID', 'UnitPackageID', 'Brand', 'Size',
       'TaxRate', 'UnitPrice', 'RecommendedRetailPrice', 'Barcode', 'Tags',
       'CustomFields', 'SearchDetails', 'LastEditedBy'),
 'SELECT StockItemID, StockItemName, SupplierID, ColorID, UnitPackageID, Brand, Size, TaxRate, UnitPrice, RecommendedRetailPrice, Barcode, Tags, CustomFields, SearchDetails, LastEditedBy FROM Warehouse.StockItems',
 true),

-- Invoices (Sales.Invoices, incremental)
('WWI', 'Sales', 'Invoices', 'bronze', 'wwi_invoices', 'incremental',
 'LastEditedWhen', 'LastEditedWhen',
 ARRAY('InvoiceID'),
 ARRAY('CustomerID', 'BillToCustomerID', 'OrderID', 'DeliveryMethodID',
       'ContactPersonID', 'AccountsPersonID', 'SalespersonPersonID', 'PackedByPersonID',
       'InvoiceDate', 'CustomerPurchaseOrderNumber', 'IsCreditNote', 'CreditNoteReason',
       'Comments', 'DeliveryInstructions', 'InternalComments', 'TotalDryItems',
       'TotalChillerItems', 'DeliveryRun', 'RunPosition', 'ReturnedDeliveryData',
       'ConfirmedDeliveryTime', 'ConfirmedReceivedBy', 'LastEditedBy', 'LastEditedWhen'),
 'SELECT InvoiceID, CustomerID, BillToCustomerID, OrderID, DeliveryMethodID, ContactPersonID, AccountsPersonID, SalespersonPersonID, PackedByPersonID, InvoiceDate, CustomerPurchaseOrderNumber, IsCreditNote, CreditNoteReason, Comments, DeliveryInstructions, InternalComments, TotalDryItems, TotalChillerItems, DeliveryRun, RunPosition, ReturnedDeliveryData, ConfirmedDeliveryTime, ConfirmedReceivedBy, LastEditedBy, LastEditedWhen FROM Sales.Invoices',
 true),

-- InvoiceLines (Sales.InvoiceLines, incremental)
('WWI', 'Sales', 'InvoiceLines', 'bronze', 'wwi_invoicelines', 'incremental',
 'LastEditedWhen', 'LastEditedWhen',
 ARRAY('InvoiceLineID'),
 ARRAY('InvoiceID', 'StockItemID', 'Description', 'PackageTypeID', 'Quantity',
       'UnitPrice', 'TaxRate', 'TaxAmount', 'LineProfit', 'ExtendedPrice',
       'LastEditedBy', 'LastEditedWhen'),
 'SELECT InvoiceLineID, InvoiceID, StockItemID, Description, PackageTypeID, Quantity, UnitPrice, TaxRate, TaxAmount, LineProfit, ExtendedPrice, LastEditedBy, LastEditedWhen FROM Sales.InvoiceLines',
 true)
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.sql("""
SELECT source_system, source_schema, source_table, target_table,
       load_type, watermark_column, custom_query
FROM etl.pipeline_metadata
ORDER BY target_table
"""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
