# Databricks notebook source
"""Valida a qualidade da Silver e da Gold do fluxo Kinesis."""

import uuid

from pyspark.sql import functions as F


# COMMAND ----------

dbutils.widgets.text(
    "silver_path",
    "s3://lakehouse-prj-db-kafka/lakehouse/silver/kinesis/sales-events/",
)
dbutils.widgets.text(
    "quality_path",
    "s3://lakehouse-prj-db-kafka/lakehouse/control/kinesis_quality/",
)
dbutils.widgets.text("gold_table", "main.default_gold.fct_streaming_sales")

silver_path = dbutils.widgets.get("silver_path")
quality_path = dbutils.widgets.get("quality_path")
gold_table = dbutils.widgets.get("gold_table")
run_id = str(uuid.uuid4())

print(f"Silver path: {silver_path}")
print(f"Gold table: {gold_table}")
print(f"Run ID: {run_id}")


# COMMAND ----------

df_silver = spark.read.format("delta").load(silver_path)
silver_rows = df_silver.count()

null_key_rows = df_silver.filter(
    "event_id IS NULL OR item_position IS NULL"
).count()

duplicate_rows = (
    df_silver
    .groupBy("event_id", "item_position")
    .count()
    .filter(F.col("count") > 1)
    .agg(F.sum(F.col("count") - 1).alias("duplicate_rows"))
    .first()["duplicate_rows"]
    or 0
)

invalid_value_rows = df_silver.filter(
    "quantity IS NULL OR quantity <= 0 "
    "OR unit_price IS NULL OR unit_price < 0"
).count()

missing_event_time_rows = df_silver.filter("event_time IS NULL").count()

gold_rows = spark.table(gold_table).count()

checks = [
    ("silver_rows_positive", silver_rows > 0, silver_rows),
    ("silver_keys_not_null", null_key_rows == 0, null_key_rows),
    ("silver_no_duplicate_keys", duplicate_rows == 0, duplicate_rows),
    ("silver_values_valid", invalid_value_rows == 0, invalid_value_rows),
    ("silver_event_time_present", missing_event_time_rows == 0, missing_event_time_rows),
    ("gold_rows_positive", gold_rows > 0, gold_rows),
]

failed_checks = [check_name for check_name, passed, _ in checks if not passed]
status = "SUCCESS" if not failed_checks else "FAILED"

print(f"Silver rows: {silver_rows}")
print(f"Gold rows: {gold_rows}")
print(f"Failed checks: {failed_checks or '<none>'}")

quality_df = spark.createDataFrame(
    [
        (
            run_id,
            "kinesis",
            "sales-events",
            check_name,
            passed,
            int(observed_value),
            status,
        )
        for check_name, passed, observed_value in checks
    ],
    schema=(
        "run_id string, source string, dataset string, check_name string, "
        "passed boolean, observed_value long, status string"
    ),
).withColumn("checked_at", F.current_timestamp())

quality_df.write.format("delta").mode("append").save(quality_path)
print(f"Quality results path: {quality_path}")

if failed_checks:
    raise ValueError(f"Streaming quality checks failed: {failed_checks}")

print("Streaming quality validation succeeded.")
