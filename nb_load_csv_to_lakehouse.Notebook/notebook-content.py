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
# META     },
# META     "warehouse": {
# META       "default_warehouse": "a45fea98-77b4-4532-b50e-1fe8903302d4",
# META       "known_warehouses": [
# META         {
# META           "id": "a45fea98-77b4-4532-b50e-1fe8903302d4",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, BooleanType, DateType,
    DecimalType, TimestampType
)

BASE_PATH = "Files/source_csv"

# ============================================================
# 1) Customers — 30 cols
# ============================================================
schema_customers = StructType([
    StructField("CustomerID", IntegerType(), False),
    StructField("CustomerName", StringType(), False),
    StructField("BillToCustomerID", IntegerType(), False),
    StructField("CustomerCategoryID", IntegerType(), False),
    StructField("BuyingGroupID", IntegerType(), True),
    StructField("PrimaryContactPersonID", IntegerType(), False),
    StructField("AlternateContactPersonID", IntegerType(), True),
    StructField("DeliveryMethodID", IntegerType(), False),
    StructField("DeliveryCityID", IntegerType(), False),
    StructField("PostalCityID", IntegerType(), False),
    StructField("CreditLimit", DecimalType(18, 2), True),
    StructField("AccountOpenedDate", DateType(), False),
    StructField("StandardDiscountPercentage", DecimalType(18, 3), False),
    StructField("IsStatementSent", BooleanType(), False),
    StructField("IsOnCreditHold", BooleanType(), False),
    StructField("PaymentDays", IntegerType(), False),
    StructField("PhoneNumber", StringType(), False),
    StructField("FaxNumber", StringType(), False),
    StructField("DeliveryRun", StringType(), True),
    StructField("RunPosition", StringType(), True),
    StructField("WebsiteURL", StringType(), False),
    StructField("DeliveryAddressLine1", StringType(), False),
    StructField("DeliveryAddressLine2", StringType(), True),
    StructField("DeliveryPostalCode", StringType(), False),
    StructField("PostalAddressLine1", StringType(), False),
    StructField("PostalAddressLine2", StringType(), True),
    StructField("PostalPostalCode", StringType(), False),
    StructField("LastEditedBy", IntegerType(), False),
    StructField("ValidFrom", TimestampType(), False),
    StructField("ValidTo", TimestampType(), False),
])

df_customers = (spark.read
    .option("delimiter", "|")
    .option("header", "false")
    .schema(schema_customers)
    .csv(f"{BASE_PATH}/Customers.csv"))

df_customers.write.mode("overwrite").saveAsTable("customers")
print(f"customers: {df_customers.count()} rows")

# ============================================================
# 2) StockItems — 24 cols
# ============================================================
schema_stockitems = StructType([
    StructField("StockItemID", IntegerType(), False),
    StructField("StockItemName", StringType(), False),
    StructField("SupplierID", IntegerType(), False),
    StructField("ColorID", IntegerType(), True),
    StructField("UnitPackageID", IntegerType(), False),
    StructField("OuterPackageID", IntegerType(), False),
    StructField("Brand", StringType(), True),
    StructField("Size", StringType(), True),
    StructField("LeadTimeDays", IntegerType(), False),
    StructField("QuantityPerOuter", IntegerType(), False),
    StructField("IsChillerStock", BooleanType(), False),
    StructField("Barcode", StringType(), True),
    StructField("TaxRate", DecimalType(18, 3), False),
    StructField("UnitPrice", DecimalType(18, 2), False),
    StructField("RecommendedRetailPrice", DecimalType(18, 2), True),
    StructField("TypicalWeightPerUnit", DecimalType(18, 3), False),
    StructField("MarketingComments", StringType(), True),
    StructField("InternalComments", StringType(), True),
    StructField("CustomFields", StringType(), True),
    StructField("Tags", StringType(), True),
    StructField("SearchDetails", StringType(), False),
    StructField("LastEditedBy", IntegerType(), False),
    StructField("ValidFrom", TimestampType(), False),
    StructField("ValidTo", TimestampType(), False),
])

df_stockitems = (spark.read
    .option("delimiter", "|")
    .option("header", "false")
    .schema(schema_stockitems)
    .csv(f"{BASE_PATH}/StockItems.csv"))

df_stockitems.write.mode("overwrite").saveAsTable("stockitems")
print(f"stockitems: {df_stockitems.count()} rows")

# ============================================================
# 3) Invoices — 25 cols
# ============================================================
schema_invoices = StructType([
    StructField("InvoiceID", IntegerType(), False),
    StructField("CustomerID", IntegerType(), False),
    StructField("BillToCustomerID", IntegerType(), False),
    StructField("OrderID", IntegerType(), True),
    StructField("DeliveryMethodID", IntegerType(), False),
    StructField("ContactPersonID", IntegerType(), False),
    StructField("AccountsPersonID", IntegerType(), False),
    StructField("SalespersonPersonID", IntegerType(), False),
    StructField("PackedByPersonID", IntegerType(), False),
    StructField("InvoiceDate", DateType(), False),
    StructField("CustomerPurchaseOrderNumber", StringType(), True),
    StructField("IsCreditNote", BooleanType(), False),
    StructField("CreditNoteReason", StringType(), True),
    StructField("Comments", StringType(), True),
    StructField("DeliveryInstructions", StringType(), True),
    StructField("InternalComments", StringType(), True),
    StructField("TotalDryItems", IntegerType(), False),
    StructField("TotalChillerItems", IntegerType(), False),
    StructField("DeliveryRun", StringType(), True),
    StructField("RunPosition", StringType(), True),
    StructField("ReturnedDeliveryData", StringType(), True),
    StructField("ConfirmedDeliveryTime", TimestampType(), True),
    StructField("ConfirmedReceivedBy", StringType(), True),
    StructField("LastEditedBy", IntegerType(), False),
    StructField("LastEditedWhen", TimestampType(), False),
])

df_invoices = (spark.read
    .option("delimiter", "|")
    .option("header", "false")
    .schema(schema_invoices)
    .csv(f"{BASE_PATH}/Invoices.csv"))

df_invoices.write.mode("overwrite").saveAsTable("invoices")
print(f"invoices: {df_invoices.count()} rows")

# ============================================================
# 4) InvoiceLines — 13 cols
# ============================================================
schema_invoicelines = StructType([
    StructField("InvoiceLineID", IntegerType(), False),
    StructField("InvoiceID", IntegerType(), False),
    StructField("StockItemID", IntegerType(), False),
    StructField("Description", StringType(), False),
    StructField("PackageTypeID", IntegerType(), False),
    StructField("Quantity", IntegerType(), False),
    StructField("UnitPrice", DecimalType(18, 2), True),
    StructField("TaxRate", DecimalType(18, 3), False),
    StructField("TaxAmount", DecimalType(18, 2), False),
    StructField("LineProfit", DecimalType(18, 2), False),
    StructField("ExtendedPrice", DecimalType(18, 2), False),
    StructField("LastEditedBy", IntegerType(), False),
    StructField("LastEditedWhen", TimestampType(), False),
])

df_invoicelines = (spark.read
    .option("delimiter", "|")
    .option("header", "false")
    .schema(schema_invoicelines)
    .csv(f"{BASE_PATH}/InvoiceLines.csv"))

df_invoicelines.write.mode("overwrite").saveAsTable("invoicelines")
print(f"invoicelines: {df_invoicelines.count()} rows")

print("\n✅ All 4 tables loaded.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
