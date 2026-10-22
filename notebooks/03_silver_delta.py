# Databricks notebook source

# COMMAND ----------

dbutils.widgets.text("source_schema", "dbo")
dbutils.widgets.text("source_table", "products")
dbutils.widgets.text("primary_keys", "")

source_schema = dbutils.widgets.get("source_schema").strip()
source_table = dbutils.widgets.get("source_table").strip()
primary_keys = [
    key.strip()
    for key in dbutils.widgets.get("primary_keys").split(",")
    if key.strip()
]

if not source_schema or not source_table:
    raise ValueError("source_schema and source_table are required.")

identifier_pattern = r"^[A-Za-z_][A-Za-z0-9_]*$"
import re

for identifier_name, identifier_value in {
    "source_schema": source_schema,
    "source_table": source_table,
    **{
        f"primary_key_{index}": key
        for index, key in enumerate(primary_keys)
    },
}.items():
    if not re.fullmatch(identifier_pattern, identifier_value):
        raise ValueError(
            f"Invalid SQL identifier in {identifier_name}: {identifier_value}"
        )

# COMMAND ----------

from pyspark.sql import functions as F
from functools import reduce

bronze_path = (
    "s3://lakehouse-prj-db-kafka/"
    f"lakehouse/bronze/azure_sql/{source_schema}/{source_table}/"
)
silver_path = (
    "s3://lakehouse-prj-db-kafka/"
    f"lakehouse/silver/azure_sql/{source_schema}/{source_table}/"
)

print(f"Source table: {source_schema}.{source_table}")
print(f"Bronze path: {bronze_path}")
print(f"Silver path: {silver_path}")
print(f"Primary keys: {primary_keys or '<none>'}")

df_bronze = spark.read.parquet(bronze_path)
bronze_rows = df_bronze.count()
print(f"Bronze rows read: {bronze_rows}")

missing_columns = [
    key for key in primary_keys if key not in df_bronze.columns
]
if missing_columns:
    raise ValueError(
        f"Primary key columns not found in Bronze: {missing_columns}"
    )

if primary_keys:
    valid_key_condition = reduce(
        lambda left, right: left & right,
        [F.col(key).isNotNull() for key in primary_keys],
    )
    null_key_rows = df_bronze.filter(~valid_key_condition).count()
    df_silver = (
        df_bronze
        .filter(valid_key_condition)
        .dropDuplicates(primary_keys)
    )
else:
    null_key_rows = 0
    df_silver = df_bronze.dropDuplicates()

silver_rows = df_silver.count()
duplicate_or_invalid_rows = bronze_rows - silver_rows
print(f"Rows with null primary key: {null_key_rows}")
print(f"Rows removed by Silver rules: {duplicate_or_invalid_rows}")

df_silver = df_silver.withColumn(
    "_silver_processed_at",
    F.current_timestamp(),
)

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(silver_path)
)

validated_rows = spark.read.format("delta").load(silver_path).count()
print(f"Silver rows written: {validated_rows}")

if validated_rows != silver_rows:
    raise RuntimeError(
        f"Silver validation failed: expected {silver_rows}, "
        f"found {validated_rows}."
    )

print("Silver Delta validation succeeded.")
