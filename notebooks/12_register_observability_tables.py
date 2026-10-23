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


def delta_path_has_columns(path: str) -> bool:
    """Verifica se o caminho possui um Delta com esquema legivel."""
    try:
        dbutils.fs.ls(path)
        return len(spark.read.format("delta").load(path).schema.fields) > 0
    except Exception:
        return False


registered_tables = []
for table in tables:
    location = f"{lakehouse_root}/control/observability/{table}/"
    if delta_path_has_columns(location):
        spark.sql(
            f"CREATE OR REPLACE TABLE {namespace}.{table} "
            f"USING DELTA LOCATION '{location}'"
        )
        registered_tables.append(table)
    else:
        # Evita manter no Unity Catalog uma tabela sem colunas.
        spark.sql(f"DROP TABLE IF EXISTS {namespace}.{table}")
        print(f"Tabela ignorada sem dados Delta: {table}")

print(f"Schema de monitoramento: {namespace}")
print(f"Raiz do Lakehouse: {lakehouse_root}")
print(f"Tabelas registradas: {len(registered_tables)}")
