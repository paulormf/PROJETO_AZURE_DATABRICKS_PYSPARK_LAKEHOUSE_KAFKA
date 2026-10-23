# Databricks notebook source
"""Registra as tabelas de observabilidade do ambiente no Unity Catalog."""

import re


dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("monitoring_schema", "dev_monitoring")
dbutils.widgets.text(
    "lakehouse_root",
    "s3://lakehouse-prj-db-kafka/lakehouse/dev",
)

catalog = dbutils.widgets.get("catalog").strip()
monitoring_schema = dbutils.widgets.get("monitoring_schema").strip()
lakehouse_root = dbutils.widgets.get("lakehouse_root").strip().rstrip("/")

identifier_pattern = r"^[A-Za-z_][A-Za-z0-9_]*$"
for name, value in {
    "catalog": catalog,
    "monitoring_schema": monitoring_schema,
}.items():
    if not re.fullmatch(identifier_pattern, value):
        raise ValueError(f"Identificador inválido do Unity Catalog: {name}={value}")

if not lakehouse_root:
    raise ValueError("lakehouse_root é obrigatório.")

namespace = f"{catalog}.{monitoring_schema}"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {namespace}")

tables = [
    "batch_ingestion_audit",
    "batch_quality",
    "streaming_quality",
    "pipeline_summary",
]

for table in tables:
    location = f"{lakehouse_root}/control/observability/{table}/"
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {namespace}.{table} "
        f"USING DELTA LOCATION '{location}'"
    )

print(f"Schema de monitoramento: {namespace}")
print(f"Raiz do Lakehouse: {lakehouse_root}")
print(f"Tabelas registradas: {len(tables)}")
