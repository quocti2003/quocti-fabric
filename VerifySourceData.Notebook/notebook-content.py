# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Server + the internal catalog name (the GUID one) from your connection string
URL = ("jdbc:sqlserver://"
       "jkscrtk7jgkunoprydpvrnoije-6nsnxfomvd6etaayiyspnju6zi.database.fabric.microsoft.com:1433;"
       "database=WWI-Source-f20bed88-5fc8-44cf-9232-1e6bac5c6ef8;")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

base = ("abfss://95db64f3-a8cc-49fc-8018-4624f6a69eca@onelake.dfs.fabric.microsoft.com/"
        "f20bed88-5fc8-44cf-9232-1e6bac5c6ef8/Tables")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import Row

results = []
for sch in notebookutils.fs.ls(base):
    if not sch.isDir:
        continue
    schema_name = sch.name.rstrip("/")
    for tbl in notebookutils.fs.ls(sch.path):
        if not tbl.isDir:
            continue
        table_name = tbl.name.rstrip("/")
        try:
            cnt = spark.read.format("delta").load(tbl.path).count()
        except Exception as e:
            cnt = -1   # -1 = folder exists but Delta not ready yet (still mirroring)
        results.append(Row(schema=schema_name, table=table_name, row_count=cnt))

counts_df = spark.createDataFrame(results).orderBy("schema", "table")
display(counts_df)
print("Tables found:", len(results),
      "| Total rows:", sum(r["row_count"] for r in results if r["row_count"] > 0))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

vl = notebookutils.variableLibrary.getLibrary("vl_wwi")

# Access variables as properties (cleanest)
ws_id              = vl.workspace_id
lakehouse_name     = vl.lakehouse_name
lakehouse_id       = vl.lakehouse_id
source_db_name     = vl.source_sqldb_name
source_db_id       = vl.source_sqldb_id
source_db_endpoint = vl.source_sqldb_endpoint
source_system      = vl.source_system_id

print(f"workspace_id:          {ws_id}")
print(f"lakehouse_name:        {lakehouse_name}")
print(f"lakehouse_id:          {lakehouse_id}")
print(f"source_sqldb_name:     {source_db_name}")
print(f"source_sqldb_id:       {source_db_id}")
print(f"source_sqldb_endpoint: {source_db_endpoint}")
print(f"source_system_id:      {source_system}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
