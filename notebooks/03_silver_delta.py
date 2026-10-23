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

from functools import reduce
import uuid

from pyspark import StorageLevel
from pyspark.sql import functions as F

bronze_path = (
    "s3://lakehouse-prj-db-kafka/"
    f"lakehouse/bronze/azure_sql/{source_schema}/{source_table}/"
)
silver_path = (
    "s3://lakehouse-prj-db-kafka/"
    f"lakehouse/silver/azure_sql/{source_schema}/{source_table}/"
)
audit_path = (
    "s3://lakehouse-prj-db-kafka/"
    f"lakehouse/control/ingestion_audit/{source_schema}/{source_table}/"
)
quality_path = (
    "s3://lakehouse-prj-db-kafka/"
    f"lakehouse/control/data_quality_results/{source_schema}/{source_table}/"
)
run_id = str(uuid.uuid4())

print(f"Source table: {source_schema}.{source_table}")
print(f"Bronze path: {bronze_path}")
print(f"Silver path: {silver_path}")
print(f"Primary keys: {primary_keys or '<none>'}")
print(f"Run ID: {run_id}")

df_bronze = spark.read.parquet(bronze_path)

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
    bronze_metrics = df_bronze.agg(
        F.count(F.lit(1)).alias("bronze_rows"),
        F.sum(
            F.when(~valid_key_condition, F.lit(1)).otherwise(F.lit(0))
        ).alias("null_key_rows"),
    ).first()
    bronze_rows = int(bronze_metrics["bronze_rows"])
    null_key_rows = int(bronze_metrics["null_key_rows"] or 0)
    df_silver = (
        df_bronze
        .filter(valid_key_condition)
        .dropDuplicates(primary_keys)
    )
else:
    bronze_rows = df_bronze.count()
    null_key_rows = 0
    df_silver = df_bronze.dropDuplicates()

print(f"Bronze rows read: {bronze_rows}")

df_silver = df_silver.persist(StorageLevel.MEMORY_AND_DISK)
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
df_silver.unpersist()

validated_rows = spark.read.format("delta").load(silver_path).count()
print(f"Silver rows written: {validated_rows}")

if validated_rows != silver_rows:
    raise RuntimeError(
        f"Silver validation failed: expected {silver_rows}, "
        f"found {validated_rows}."
    )

audit_record = spark.createDataFrame(
    [
        (
            run_id,
            source_schema,
            source_table,
            bronze_rows,
            silver_rows,
            null_key_rows,
            duplicate_or_invalid_rows,
            "SUCCESS",
        )
    ],
    schema=(
        "run_id string, source_schema string, source_table string, "
        "bronze_rows long, silver_rows long, null_key_rows long, "
        "rows_removed long, status string"
    ),
).withColumn("processed_at", F.current_timestamp())

quality_records = [
    (
        run_id,
        source_schema,
        source_table,
        "primary_key_not_null",
        not primary_keys or null_key_rows == 0,
        null_key_rows,
    ),
    (
        run_id,
        source_schema,
        source_table,
        "silver_row_count_positive",
        silver_rows > 0,
        silver_rows,
    ),
    (
        run_id,
        source_schema,
        source_table,
        "silver_write_reconciliation",
        validated_rows == silver_rows,
        validated_rows,
    ),
]
quality_record_df = spark.createDataFrame(
    quality_records,
    schema=(
        "run_id string, source_schema string, source_table string, "
        "check_name string, passed boolean, observed_value long"
    ),
).withColumn("checked_at", F.current_timestamp())

audit_record.write.format("delta").mode("append").save(audit_path)
quality_record_df.write.format("delta").mode("append").save(quality_path)

print(f"Audit path: {audit_path}")
print(f"Quality path: {quality_path}")
print("Silver Delta validation succeeded.")
