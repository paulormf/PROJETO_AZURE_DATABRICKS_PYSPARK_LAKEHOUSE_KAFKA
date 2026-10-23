# Databricks notebook source
"""Registra as tabelas Delta do ambiente atual no Unity Catalog."""

import re


dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("silver_schema", "dev_silver")
dbutils.widgets.text("governed_schema", "dev_governed")
dbutils.widgets.text(
    "lakehouse_root",
    "s3://lakehouse-prj-db-kafka/lakehouse/dev",
)

catalog = dbutils.widgets.get("catalog").strip()
silver_schema = dbutils.widgets.get("silver_schema").strip()
governed_schema = dbutils.widgets.get("governed_schema").strip()
lakehouse_root = dbutils.widgets.get("lakehouse_root").strip().rstrip("/")

identifier_pattern = r"^[A-Za-z_][A-Za-z0-9_]*$"
for name, value in {
    "catalog": catalog,
    "silver_schema": silver_schema,
    "governed_schema": governed_schema,
}.items():
    if not re.fullmatch(identifier_pattern, value):
        raise ValueError(f"Invalid Unity Catalog identifier: {name}={value}")

if not lakehouse_root:
    raise ValueError("lakehouse_root is required.")

tables = [
    "orders", "order_items", "products", "categories", "brands",
    "sales_channels", "customers", "customer_contacts",
    "customer_addresses", "employees", "suppliers",
]

catalog_schema = f"{catalog}.{silver_schema}"
governed_namespace = f"{catalog}.{governed_schema}"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {governed_namespace}")

for table in tables:
    location = f"{lakehouse_root}/silver/azure_sql/dbo/{table}/"
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {catalog_schema}.{table} "
        f"USING DELTA LOCATION '{location}'"
    )

kinesis_location = f"{lakehouse_root}/silver/kinesis/sales-events/"
spark.sql(
    f"CREATE TABLE IF NOT EXISTS {catalog_schema}.kinesis_sales_events "
    f"USING DELTA LOCATION '{kinesis_location}'"
)

governed_views = {
    "v_customers_analytics": f"""
        SELECT customer_id, customer_type, YEAR(birth_date) AS birth_year,
               gender, status, created_at, updated_at
        FROM {catalog_schema}.customers
    """,
    "v_customer_contacts_analytics": f"""
        SELECT customer_id, contact_type, is_primary, created_at, updated_at
        FROM {catalog_schema}.customer_contacts
    """,
    "v_customer_addresses_analytics": f"""
        SELECT customer_id, address_type, city, state, country,
               is_primary, created_at, updated_at
        FROM {catalog_schema}.customer_addresses
    """,
    "v_employees_analytics": f"""
        SELECT employee_id, department_id, manager_id, hire_date,
               termination_date,
               CASE
                   WHEN salary IS NULL THEN 'UNKNOWN'
                   WHEN salary < 3000 THEN 'A'
                   WHEN salary < 7000 THEN 'B'
                   WHEN salary < 15000 THEN 'C'
                   ELSE 'D'
               END AS salary_band,
               status, created_at, updated_at
        FROM {catalog_schema}.employees
    """,
    "v_suppliers_analytics": f"""
        SELECT supplier_id, company_name, trade_name, city, state,
               status, created_at, updated_at
        FROM {catalog_schema}.suppliers
    """,
}

for view_name, view_query in governed_views.items():
    spark.sql(
        f"CREATE OR REPLACE VIEW {governed_namespace}.{view_name} AS "
        f"{view_query}"
    )

print(f"Catálogo: {catalog}")
print(f"Schema Silver: {catalog_schema}")
print(f"Schema Governed: {governed_namespace}")
print(f"Raiz Lakehouse: {lakehouse_root}")
print(f"Tabelas Silver registradas: {len(tables) + 1}")
print(f"Views Governed registradas: {len(governed_views)}")
