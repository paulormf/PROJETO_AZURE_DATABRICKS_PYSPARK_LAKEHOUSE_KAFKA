# Databricks notebook source

# COMMAND ----------

dbutils.widgets.text("source_schema", "dbo")
dbutils.widgets.text("source_table", "order_items")
dbutils.widgets.text("primary_keys", "order_item_id")

source_schema = dbutils.widgets.get("source_schema").strip()
source_table = dbutils.widgets.get("source_table").strip()
primary_keys = [
    key.strip()
    for key in dbutils.widgets.get("primary_keys").split(",")
    if key.strip()
]

if not source_schema or not source_table:
    raise ValueError("source_schema and source_table are required.")

# COMMAND ----------

from functools import reduce

from pyspark.sql import functions as F

bronze_path = (
    "s3://lakehouse-prj-db-kafka/"
    f"lakehouse/bronze/azure_sql/{source_schema}/{source_table}/"
)

def read_spark_conf(conf_name: str) -> str:
    try:
        return spark.conf.get(conf_name)
    except Exception:
        return "<managed or unavailable in Serverless>"


print(f"Source: {source_schema}.{source_table}")
print(f"Bronze path: {bronze_path}")
print(f"Primary keys: {primary_keys or '<none>'}")
print(
    "spark.sql.shuffle.partitions: "
    f"{read_spark_conf('spark.sql.shuffle.partitions')}"
)
print(
    "spark.sql.adaptive.enabled: "
    f"{read_spark_conf('spark.sql.adaptive.enabled')}"
)

df_bronze = spark.read.parquet(bronze_path)
non_empty_partitions = (
    df_bronze
    .select(F.spark_partition_id().alias("partition_id"))
    .distinct()
    .count()
)
print(f"Input non-empty partitions: {non_empty_partitions}")
print(f"Input rows: {df_bronze.count()}")

if primary_keys:
    valid_key_condition = reduce(
        lambda left, right: left & right,
        [F.col(key).isNotNull() for key in primary_keys],
    )
    df_candidate = (
        df_bronze
        .filter(valid_key_condition)
        .dropDuplicates(primary_keys)
    )
else:
    df_candidate = df_bronze.dropDuplicates()

print("Current Silver transformation plan:")
df_candidate.explain("formatted")

print("Candidate rows after current Silver rules:")
print(df_candidate.count())
