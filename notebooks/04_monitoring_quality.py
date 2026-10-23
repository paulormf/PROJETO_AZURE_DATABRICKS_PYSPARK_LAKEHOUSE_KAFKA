# Databricks notebook source

# COMMAND ----------

from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


SOURCE_SCHEMA = "dbo"
TABLES = [
    "brands",
    "campaigns",
    "carriers",
    "categories",
    "customer_addresses",
    "customer_campaigns",
    "customer_contacts",
    "customers",
    "departments",
    "employees",
    "inventory_balance",
    "inventory_movements",
    "order_items",
    "orders",
    "payment_transactions",
    "payments",
    "products",
    "returns",
    "sales_channels",
    "shipments",
    "suppliers",
    "warehouses",
]


def read_delta_paths(paths: list[str]) -> DataFrame | None:
    frames = []
    for path in paths:
        try:
            frames.append(spark.read.format("delta").load(path))
        except Exception as error:
            print(f"Path not available yet: {path} ({error.__class__.__name__})")

    if not frames:
        return None
    return reduce(
        lambda left, right: left.unionByName(right, allowMissingColumns=True),
        frames,
    )


audit_paths = [
    "s3://lakehouse-prj-db-kafka/"
    f"lakehouse/control/ingestion_audit/{SOURCE_SCHEMA}/{table}/"
    for table in TABLES
]
quality_paths = [
    "s3://lakehouse-prj-db-kafka/"
    f"lakehouse/control/data_quality_results/{SOURCE_SCHEMA}/{table}/"
    for table in TABLES
]

df_audit = read_delta_paths(audit_paths)
df_quality = read_delta_paths(quality_paths)

if df_audit is None and df_quality is None:
    raise FileNotFoundError("No audit or quality Delta paths were found.")

# COMMAND ----------

GOVERNED_VIEWS = [
    "main.governed.v_customers_analytics",
    "main.governed.v_customer_contacts_analytics",
    "main.governed.v_customer_addresses_analytics",
    "main.governed.v_employees_analytics",
    "main.governed.v_suppliers_analytics",
]
FORBIDDEN_ANALYTICS_COLUMNS = {
    "cpf",
    "cnpj",
    "email",
    "phone",
    "contact_value",
    "street",
    "number",
    "complement",
    "neighborhood",
    "zip_code",
    "salary",
}

governance_results = []
for view_name in GOVERNED_VIEWS:
    described = spark.sql(f"DESCRIBE TABLE {view_name}")
    view_columns = [
        row["col_name"].strip().lower()
        for row in described.select("col_name").collect()
        if row["col_name"] and not row["col_name"].startswith("#")
    ]
    forbidden_columns = sorted(
        set(view_columns).intersection(FORBIDDEN_ANALYTICS_COLUMNS)
    )
    governance_results.append(
        (
            view_name,
            ", ".join(view_columns),
            ", ".join(forbidden_columns),
            len(forbidden_columns) == 0,
        )
    )

df_governance = spark.createDataFrame(
    governance_results,
    ["view_name", "exposed_columns", "forbidden_columns", "passed"],
)

print("Governance checks")
display(df_governance)

if df_governance.filter(~F.col("passed")).limit(1).count() > 0:
    raise ValueError("A governed analytics view exposes a forbidden sensitive column.")

# COMMAND ----------

if df_audit is not None:
    print("Ingestion audit")
    display(
        df_audit
        .orderBy(F.col("processed_at").desc())
    )

    print("Audit summary by status")
    display(
        df_audit
        .groupBy("status")
        .count()
        .orderBy("status")
    )

if df_quality is not None:
    print("Data quality results")
    display(
        df_quality
        .orderBy(F.col("checked_at").desc())
    )

    print("Failed quality checks")
    display(
        df_quality
        .filter(~F.col("passed"))
        .orderBy(F.col("checked_at").desc())
    )
