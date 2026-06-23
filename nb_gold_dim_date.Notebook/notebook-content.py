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

# Output: gold.dim_date populated with one row per day in range (START DATE - END DATE)
#         every columns in a row pre-computed/already computed to query faster in BI tools
#         plus one default row (DateKey = -1) for unknown dates.

from pyspark.sql.functions import (
    col, lit, expr,
    year, month, dayofmonth, dayofweek, dayofyear,
    weekofyear, quarter, date_format,
    when, upper, trunc, floor
)
from datetime import date

# Target table
GOLD_TABLE = "gold.dim_date"

# Date range — covers all WWI business dates plus future buffer.
# Earliest fact date in Silver: 2013-01-01 (InvoiceDate, AccountOpenedDate)
# 2010-2030 gives 3-year backward buffer + 14-year forward buffer
START_DATE = "2010-01-01"
END_DATE   = "2030-12-31"

# Every dim has a "n/a" row at surrogate key = -1.
# Fact rows with NULL/unknown date column point to this row, so joins never break.
DEFAULT_DATE_KEY    = -1                        # the surrogate key for the "n/a" row. Industry standard.
DEFAULT_DATE        = date(2999, 12, 31)        # recognizable as "unknown."   
DEFAULT_DATE_STRING = "n/a"                     # string label for report

print(f"Generating dim_date for range: {START_DATE} → {END_DATE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# GENERATE DATE RANGE
# PySpark equivalent: sequence() generates an array of all dates in range START_DATE -> END_DATE,
# explode() turns the array into one row per date (1 row/day). Single Spark job, processed in parallel.

date_range_df = (
    spark.range(1)   # single driver row — sequence needs SOME input
    .select(
        expr(f"sequence(to_date('{START_DATE}'), to_date('{END_DATE}'), interval 1 day) AS dates")
    )
    .selectExpr("explode(dates) AS Date")
)

print(f"Date rows generated: {date_range_df.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# COMPUTE CALENDAR ATTRIBUTES

from pyspark.sql.functions import (
    col, lit, expr,
    year, month, dayofmonth, dayofweek, dayofyear,
    weekofyear, quarter, date_format,
    when, upper, trunc, floor
)

dim_date_df = (
    date_range_df

    # DateKey — YYYYMMDD integer
    # 2026-05-22 → 2026*10000 + 5*100 + 22 = 20260522. (simply is 6 is ten hundred position)
    .withColumn(
        "DateKey",
        (year("Date") * 10000 + month("Date") * 100 + dayofmonth("Date")).cast("int")
    )

    # DateString — day-of-month as string "2026-05-22"	- text version
    .withColumn("DateString", dayofmonth("Date").cast("string"))

    # Day + DaySuffix (English ordinals: 1st, 2nd, 3rd, ...)
    .withColumn("Day", dayofmonth("Date"))
    .withColumn(
        "DaySuffix",
        when(col("Day").isin(1, 21, 31), "st") # If Day is 1, 21, or 31 → "st"
        .when(col("Day").isin(2, 22), "nd")
        .when(col("Day").isin(3, 23), "rd")
        .otherwise("th")
    )

    # Weekday number (1=Sunday, 7=Saturday — Spark/SQL Server convention)
    .withColumn("Weekday", dayofweek("Date"))

    # Weekday names (English locale)
    .withColumn("WeekDayName",             date_format("Date", "EEEE")) # standard pattern Java/Spark for full name in Engrisk
    .withColumn("WeekDayName_Short",       upper(date_format("Date", "EEE"))) # → "Fri" (built-in return with upper 1st character).
    .withColumn("WeekDayName_FirstLetter", date_format("Date", "EEE").substr(1, 1))

    # DOWInMonth — "Nth occurrence of this weekday in the month"
    .withColumn("DOWInMonth", (floor((dayofmonth("Date") - 1) / 7) + 1).cast("int"))

    # DayOfYear
    .withColumn("DayOfYear", dayofyear("Date"))

    .withColumn("WeekOfMonth", (floor((dayofmonth("Date") - 1) / 7) + 1).cast("int"))

    # WeekOfYear (ISO week)
    .withColumn("WeekOfYear", weekofyear("Date"))

    # Month + names (English)
    .withColumn("Month", month("Date"))
    .withColumn("MonthName",             date_format("Date", "MMMM"))
    .withColumn("MonthName_Short",       upper(date_format("Date", "MMM")))
    .withColumn("MonthName_FirstLetter", date_format("Date", "MMM").substr(1, 1))

    # Quarter + name (English: First / Second / Third / Fourth)
    .withColumn("Quarter", quarter("Date"))
    .withColumn(
        "QuarterName",
        when(col("Quarter") == 1, "First")
        .when(col("Quarter") == 2, "Second")
        .when(col("Quarter") == 3, "Third")
        .when(col("Quarter") == 4, "Fourth")
    )

    # Year + composites
    .withColumn("Year", year("Date"))
    .withColumn("YearMonthNo", (year("Date") * 100 + month("Date")).cast("int"))
    .withColumn("YearMonthName", date_format("Date", "yyyy MMM"))   # "2026 May"

    # Weekend we use dayofweek 1=Sun, 7=Sat
    .withColumn(
        "IsWeekend",
        when(dayofweek("Date").isin(1, 7), 1).otherwise(0)
    )
)

print(f"Calendar rows ready: {dim_date_df.count()}")
print("Sample (first 3 rows):")
display(dim_date_df.orderBy("Date").limit(3))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# APPEND DEFAULT ROW + WRITE TO DELTA (overwrite mode)
#
# Default row: DateKey = -1, sentinels in every other column.
# Purpose: facts with NULL/unknown date columns point to skey = -1
# instead of NULL → joins never break, reports show 'n/a' explicitly.
#
# Write mode: overwrite (TRUNCATE-then-write semantics, matches

# Final column order — must match DDL exactly so .saveAsTable
# aligns columns correctly by position
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType
COLS = [
    "DateKey", "Date", "DateString",
    "Day", "DaySuffix",
    "Weekday", "WeekDayName", "WeekDayName_Short", "WeekDayName_FirstLetter",
    "DOWInMonth", "DayOfYear",
    "WeekOfMonth", "WeekOfYear",
    "Month", "MonthName", "MonthName_Short", "MonthName_FirstLetter",
    "Quarter", "QuarterName",
    "Year", "YearMonthNo", "YearMonthName",
    "IsWeekend"
]

dim_date_final = dim_date_df.select(*COLS)

# Explicit schema for default row — types MUST match DDL (Integer not Long)
default_schema = StructType([
    StructField("DateKey",                 IntegerType(), False),
    StructField("Date",                    DateType(),    False),
    StructField("DateString",              StringType(),  False),
    StructField("Day",                     IntegerType(), True),
    StructField("DaySuffix",               StringType(),  True),
    StructField("Weekday",                 IntegerType(), True),
    StructField("WeekDayName",             StringType(),  True),
    StructField("WeekDayName_Short",       StringType(),  True),
    StructField("WeekDayName_FirstLetter", StringType(),  True),
    StructField("DOWInMonth",              IntegerType(), True),
    StructField("DayOfYear",               IntegerType(), True),
    StructField("WeekOfMonth",             IntegerType(), True),
    StructField("WeekOfYear",              IntegerType(), True),
    StructField("Month",                   IntegerType(), True),
    StructField("MonthName",               StringType(),  True),
    StructField("MonthName_Short",         StringType(),  True),
    StructField("MonthName_FirstLetter",   StringType(),  True),
    StructField("Quarter",                 IntegerType(), True),
    StructField("QuarterName",             StringType(),  True),
    StructField("Year",                    IntegerType(), True),
    StructField("YearMonthNo",             IntegerType(), True),
    StructField("YearMonthName",           StringType(),  True),
    StructField("IsWeekend",               IntegerType(), True),
])

default_row_df = spark.createDataFrame(
    [(
        DEFAULT_DATE_KEY,
        DEFAULT_DATE,
        DEFAULT_DATE_STRING,
        -1, "",
        -1, "", "", "",
        -1, -1,
        -1, -1,
        -1, "", "", "",
        -1, "",
        -1, -1, "",
        -1
    )],
    schema=default_schema
)

# Union default + generated dates
final_df = default_row_df.unionByName(dim_date_final)

# Write — overwrite replaces the entire table content
final_df.write.format("delta").mode("overwrite").saveAsTable(GOLD_TABLE)

print(f"Wrote {final_df.count()} rows to {GOLD_TABLE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# VERIFY — output as Fabric tables

result = spark.read.table(GOLD_TABLE)

print(f"=== Total rows: {result.count()} ===")

print("\n=== Default row (DateKey = -1) ===")
display(result.filter(col("DateKey") == -1))

print("\n=== Sample: today's DateKey (2026-05-21) ===")
display(result.filter(col("DateKey") == 20260521))

print("\n=== Sample: first 3 dates in range ===")
display(result.filter(col("DateKey") != -1).orderBy("Date").limit(3))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("\n=== Verify DOWInMonth: 4 Thursdays in May 2026 ===")
display(
    result.filter(col("DateKey").isin(20260507, 20260514, 20260521, 20260528))
          .orderBy("Date")
          .select("Date", "WeekDayName", "Day", "DOWInMonth")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
