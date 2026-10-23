# Databricks notebook source
"""Consolida metricas operacionais do batch e do Streaming."""

from functools import reduce
import uuid

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


TABLES = [
    "brands", "campaigns", "carriers", "categories",
    "customer_addresses", "customer_campaigns", "customer_contacts",
    "customers", "departments", "employees", "inventory_balance",
    "inventory_movements", "order_items", "orders",
    "payment_transactions", "payments", "products", "returns",
    "sales_channels", "shipments", "suppliers", "warehouses",
]

BATCH_AUDIT_OUTPUT = "s3://lakehouse-prj-db-kafka/lakehouse/control/observability/batch_ingestion_audit/"
BATCH_QUALITY_OUTPUT = "s3://lakehouse-prj-db-kafka/lakehouse/control/observability/batch_quality/"
STREAMING_QUALITY_PATH = "s3://lakehouse-prj-db-kafka/lakehouse/control/kinesis_quality/"
STREAMING_OUTPUT = "s3://lakehouse-prj-db-kafka/lakehouse/control/observability/streaming_quality/"


def read_delta_paths(paths: list[str]) -> DataFrame | None:
    frames = []
    for path in paths:
        try:
            frames.append(spark.read.format("delta").load(path))
        except Exception:
            continue
    if not frames:
        return None
    return reduce(
        lambda left, right: left.unionByName(right, allowMissingColumns=True),
        frames,
    )


batch_audit = read_delta_paths([
    f"s3://lakehouse-prj-db-kafka/lakehouse/control/ingestion_audit/dbo/{table}/"
    for table in TABLES
])
batch_quality = read_delta_paths([
    f"s3://lakehouse-prj-db-kafka/lakehouse/control/data_quality_results/dbo/{table}/"
    for table in TABLES
])

if batch_audit is not None:
    (
        batch_audit
        .withColumn("pipeline", F.lit("batch_azure_sql"))
        .withColumn("observed_at", F.current_timestamp())
        .write.format("delta").mode("overwrite").save(BATCH_AUDIT_OUTPUT)
    )
    print(f"Batch audit rows: {batch_audit.count()}")

if batch_quality is not None:
    (
        batch_quality
        .withColumn("pipeline", F.lit("batch_azure_sql"))
        .withColumn("observed_at", F.current_timestamp())
        .write.format("delta").mode("overwrite").save(BATCH_QUALITY_OUTPUT)
    )
    print(f"Batch quality rows: {batch_quality.count()}")

try:
    streaming_quality = spark.read.format("delta").load(STREAMING_QUALITY_PATH)
except Exception:
    streaming_quality = None

if streaming_quality is not None:
    (
        streaming_quality
        .withColumn("pipeline", F.lit("streaming_kinesis"))
        .withColumn("observed_at", F.current_timestamp())
        .write.format("delta").mode("overwrite").save(STREAMING_OUTPUT)
    )
    print(f"Streaming quality rows: {streaming_quality.count()}")

summary = spark.createDataFrame(
    [
        (
            str(uuid.uuid4()),
            "batch_azure_sql",
            "audit_available",
            batch_audit is not None,
        ),
        (
            str(uuid.uuid4()),
            "batch_azure_sql",
            "quality_available",
            batch_quality is not None,
        ),
        (
            str(uuid.uuid4()),
            "streaming_kinesis",
            "quality_available",
            streaming_quality is not None,
        ),
    ],
    "run_id string, pipeline string, metric_name string, metric_value boolean",
).withColumn("observed_at", F.current_timestamp())

summary.write.format("delta").mode("append").save(
    "s3://lakehouse-prj-db-kafka/lakehouse/control/observability/pipeline_summary/"
)

display(summary)
print("Observability metrics consolidated successfully.")
