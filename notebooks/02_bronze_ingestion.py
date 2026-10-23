# Databricks notebook source

# COMMAND ----------

dbutils.widgets.text("source_schema", "dbo")
dbutils.widgets.text("source_table", "carriers")
dbutils.widgets.text("load_mode", "full")
dbutils.widgets.text("watermark_column", "")
dbutils.widgets.text("last_watermark", "")
dbutils.widgets.text(
    "watermark_path",
    "s3://lakehouse-prj-db-kafka/lakehouse/control/bronze_watermarks_v2/",
)

source_schema = dbutils.widgets.get("source_schema").strip()
source_table = dbutils.widgets.get("source_table").strip()
load_mode = dbutils.widgets.get("load_mode").strip().lower()
watermark_column = dbutils.widgets.get("watermark_column").strip()
last_watermark = dbutils.widgets.get("last_watermark").strip()
watermark_root = dbutils.widgets.get("watermark_path").strip()

if load_mode not in {"full", "incremental"}:
    raise ValueError("load_mode must be 'full' or 'incremental'.")

if load_mode == "incremental" and not watermark_column:
    raise ValueError(
        "watermark_column is required for incremental loads."
    )

# Estes valores são controlados pelos parâmetros do Job, e não pela entrada do usuário.
identifier_pattern = r"^[A-Za-z_][A-Za-z0-9_]*$"
import re

for identifier_name, identifier_value in {
    "source_schema": source_schema,
    "source_table": source_table,
    "watermark_column": watermark_column,
}.items():
    if identifier_value and not re.fullmatch(identifier_pattern, identifier_value):
        raise ValueError(
            f"Invalid SQL identifier in {identifier_name}: {identifier_value}"
        )

watermark_path = (
    f"{watermark_root.rstrip('/')}/{source_schema}/{source_table}/"
)

# COMMAND ----------

from pyspark.sql import functions as F

sql_host = dbutils.widgets.get("sql_host").strip()
sql_database = dbutils.widgets.get("sql_database").strip()

if not sql_host or not sql_database:
    raise ValueError("sql_host and sql_database are required Job parameters.")

sql_username = dbutils.secrets.get(
    scope="azsql",
    key="username",
)

sql_password = dbutils.secrets.get(
    scope="azsql",
    key="password",
)

# COMMAND ----------

def path_exists(path: str) -> bool:
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False


def delta_table_exists(path: str) -> bool:
    return path_exists(f"{path.rstrip('/')}/_delta_log/")


control_watermark = ""
if load_mode == "incremental" and not last_watermark:
    if delta_table_exists(watermark_path):
        df_watermarks = (
            spark.read.format("delta").load(watermark_path)
            .where(
                (F.col("source_schema") == source_schema)
                & (F.col("source_table") == source_table)
            )
            .orderBy(F.col("updated_at").desc())
        )
        watermark_rows = df_watermarks.select("last_watermark").limit(1).collect()
        if watermark_rows:
            control_watermark = watermark_rows[0]["last_watermark"] or ""

    if control_watermark:
        last_watermark = control_watermark
        print(f"Watermark loaded from control table: {last_watermark}")
    else:
        print("No previous watermark found in control table.")

# Um parâmetro de execução continua disponível como substituição explícita para testes.
print(f"Watermark control path: {watermark_path}")

qualified_table = f"[{source_schema}].[{source_table}]"

if load_mode == "incremental" and last_watermark:
    escaped_watermark = last_watermark.replace("'", "''")
    query = (
        f"SELECT * FROM {qualified_table} "
        f"WHERE [{watermark_column}] > '{escaped_watermark}'"
    )
else:
    query = f"SELECT * FROM {qualified_table}"

print(f"Source table: {source_schema}.{source_table}")
print(f"Load mode: {load_mode}")
print(f"Query watermark: {last_watermark or '<initial load>'}")

df_source = (
    spark.read
    .format("sqlserver")
    .option("host", sql_host)
    .option("port", "1433")
    .option("database", sql_database)
    .option("user", sql_username)
    .option("password", sql_password)
    .option("query", query)
    .load()
)

rows_read = df_source.count()
print(f"Rows read: {rows_read}")

if rows_read == 0:
    if load_mode == "incremental" and last_watermark:
        print("No new rows found for the supplied watermark.")
        dbutils.notebook.exit("NO_NEW_DATA")
    raise ValueError("The initial load returned no rows.")

# COMMAND ----------

bronze_path = (
    "s3://lakehouse-prj-db-kafka/"
    f"lakehouse/bronze/azure_sql/{source_schema}/{source_table}/"
)

df_bronze = (
    df_source
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_schema", F.lit(source_schema))
    .withColumn("_source_table", F.lit(source_table))
    .withColumn("_load_mode", F.lit(load_mode))
)

# A primeira execução incremental inicializa o destino. As execuções seguintes adicionam dados.
write_mode = "overwrite" if load_mode == "full" or not last_watermark else "append"

(
    df_bronze.write
    .mode(write_mode)
    .parquet(bronze_path)
)

print(f"Bronze path: {bronze_path}")
print(f"Write mode: {write_mode}")

if load_mode == "incremental":
    max_watermark = (
        df_source
        .select(F.max(F.col(watermark_column)).alias("max_watermark"))
        .collect()[0]["max_watermark"]
    )
    print(f"Max watermark in batch: {max_watermark}")

    if max_watermark is not None:
        watermark_record = spark.createDataFrame(
            [
                (
                    source_schema,
                    source_table,
                    watermark_column,
                    str(max_watermark),
                )
            ],
            schema=(
                "source_schema string, source_table string, "
                "watermark_column string, last_watermark string"
            ),
        ).withColumn("updated_at", F.current_timestamp())

        if delta_table_exists(watermark_path):
            from delta.tables import DeltaTable

            (
                DeltaTable.forPath(spark, watermark_path)
                .alias("target")
                .merge(
                    watermark_record.alias("source"),
                    "target.source_schema = source.source_schema "
                    "AND target.source_table = source.source_table",
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            watermark_record.write.format("delta").mode("overwrite").save(
                watermark_path
            )

        print(f"Watermark persisted: {watermark_path}")

output_entries = dbutils.fs.ls(bronze_path)
parquet_files = [
    entry.path
    for entry in output_entries
    if entry.path.endswith(".parquet")
]

print(f"Parquet files in Bronze: {len(parquet_files)}")
for parquet_file in parquet_files:
    print(parquet_file)

if not parquet_files:
    raise FileNotFoundError(
        f"No Parquet files found at {bronze_path}."
    )
