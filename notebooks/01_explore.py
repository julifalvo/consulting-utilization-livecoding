# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 0: look at the data before writing a transform
# MAGIC Works on Databricks (point `LANDING` at the UC volume) or locally in the dev container
# MAGIC (`python notebooks/01_explore.py`).

# COMMAND ----------

import os

from pyspark.sql import functions as F

from utilization.spark import get_spark

spark = get_spark()
LANDING = os.getenv("UTIL_LANDING", "data/raw")

# COMMAND ----------

# Read with inference ONLY to explore. What type does Spark pick for `hours`? Why is that a trap?
# .cache(): Spark refuses queries on raw JSON that only reference `_corrupt_record`
# (UNSUPPORTED_FEATURE.QUERY_ONLY_CORRUPT_RECORD_COLUMN). Bronze avoids it by writing first.
raw = (
    spark.read.json(f"{LANDING}/timesheets/*.json")
    .withColumn("_file", F.element_at(F.split(F.col("_metadata.file_path"), "/"), -1))
    .cache()
)
raw.printSchema()

# COMMAND ----------

# Which columns exist in which file? (schema drift)
for col in ["is_deleted", "activity_type", "_corrupt_record"]:
    if col in raw.columns:
        raw.filter(F.col(col).isNotNull()).groupBy("_file").count().orderBy("_file").show(truncate=False)

# COMMAND ----------

# Duplicated entry ids: re-sends or corrections?
dupes = raw.groupBy("entry_id").agg(
    F.count("*").alias("versions"),
    F.countDistinct("hours").alias("distinct_hours"),
    F.collect_set("updated_at").alias("updated_at"),
)
dupes.filter("versions > 1").orderBy(F.desc("versions")).show(10, truncate=False)

# COMMAND ----------

# What do hours and timestamps look like?
raw.select("hours").distinct().orderBy("hours").show(50)
raw.select(F.regexp_extract("updated_at", r"(Z|[+-]\d\d:\d\d)$", 1).alias("offset")).groupBy(
    "offset"
).count().show()

# COMMAND ----------

# Reference data: do all codes in the timesheets exist? Who has more than one HR row?
emp = spark.read.option("header", True).csv(f"{LANDING}/reference/employees.csv")
eng = spark.read.option("header", True).csv(f"{LANDING}/reference/engagements.csv")
raw.join(eng, "engagement_code", "left_anti").select("entry_id", "engagement_code").show()
raw.join(emp, "employee_id", "left_anti").select("entry_id", "employee_id").show()
emp.groupBy("employee_id").count().filter("count > 1").join(emp, "employee_id").show()
