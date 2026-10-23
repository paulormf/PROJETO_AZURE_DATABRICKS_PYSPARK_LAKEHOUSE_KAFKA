# Databricks notebook source
"""Consolida metricas operacionais do batch e do Streaming."""

from functools import reduce
import uuid

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


dbutils.widgets.text(
    "lakehouse_root",
    "s3://lakehouse-prj-db-kafka/lakehouse/dev",
)
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("monitoring_schema", "dev_monitoring")
LAKEHOUSE_ROOT = dbutils.widgets.get("lakehouse_root").strip().rstrip("/")
CATALOG = dbutils.widgets.get("catalog").strip()
MONITORING_SCHEMA = dbutils.widgets.get("monitoring_schema").strip()
if not LAKEHOUSE_ROOT:
    raise ValueError("lakehouse_root is required.")
if not CATALOG or not MONITORING_SCHEMA:
    raise ValueError("catalog and monitoring_schema are required.")


TABLES = [
    "brands", "campaigns", "carriers", "categories",
    "customer_addresses", "customer_campaigns", "customer_contacts",
    "customers", "departments", "employees", "inventory_balance",
    "inventory_movements", "order_items", "orders",
    "payment_transactions", "payments", "products", "returns",
    "sales_channels", "shipments", "suppliers", "warehouses",
]

BATCH_AUDIT_OUTPUT = f"{LAKEHOUSE_ROOT}/control/observability/batch_ingestion_audit/"
BATCH_QUALITY_OUTPUT = f"{LAKEHOUSE_ROOT}/control/observability/batch_quality/"
STREAMING_QUALITY_PATH = f"{LAKEHOUSE_ROOT}/control/kinesis_quality/"
STREAMING_OUTPUT = f"{LAKEHOUSE_ROOT}/control/observability/streaming_quality/"


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


def path_exists(path: str) -> bool:
    """Verifica o caminho antes da leitura lazy do Spark."""
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False


batch_audit = read_delta_paths([
    f"{LAKEHOUSE_ROOT}/control/ingestion_audit/dbo/{table}/"
    for table in TABLES
])
batch_quality = read_delta_paths([
    f"{LAKEHOUSE_ROOT}/control/data_quality_results/dbo/{table}/"
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

if path_exists(STREAMING_QUALITY_PATH):
    streaming_quality = spark.read.format("delta").load(STREAMING_QUALITY_PATH)
else:
    streaming_quality = None
    print(
        "Qualidade de streaming ainda não disponível; "
        f"caminho ausente: {STREAMING_QUALITY_PATH}"
    )

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
    f"{LAKEHOUSE_ROOT}/control/observability/pipeline_summary/"
)

display(summary)
print("Observability metrics consolidated successfully.")
