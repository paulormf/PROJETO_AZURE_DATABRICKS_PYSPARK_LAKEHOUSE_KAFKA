# Databricks notebook source

# COMMAND ----------

# Preencha somente estes dois valores. Nunca coloque usuário ou senha aqui.
dbutils.widgets.text("sql_host", "")
dbutils.widgets.text("sql_database", "")

sql_host = dbutils.widgets.get("sql_host").strip()
sql_database = dbutils.widgets.get("sql_database").strip()

if not sql_host or not sql_database:
    raise ValueError("Os parâmetros sql_host e sql_database são obrigatórios.")

source_table = "dbo.products"
test_output_path = (
    "s3://lakehouse-prj-db-kafka/"
    "lakehouse/bronze/azure_sql/dbo/products/_test/"
)

# COMMAND ----------

from pyspark.sql import functions as F

sql_username = dbutils.secrets.get(
    scope="azsql",
    key="username",
)

sql_password = dbutils.secrets.get(
    scope="azsql",
    key="password",
)

# COMMAND ----------

query = f"SELECT TOP (10) * FROM {source_table}"

df_test = (
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

display(df_test)

rows_read = df_test.count()
print(f"Rows read from Azure SQL: {rows_read}")

if rows_read == 0:
    raise ValueError(f"The source table {source_table} returned no rows.")

# COMMAND ----------

(
    df_test
    .withColumn("_ingested_at", F.current_timestamp())
    .write
    .mode("overwrite")
    .parquet(test_output_path)
)

print(f"Parquet gravado em: {test_output_path}")

output_entries = dbutils.fs.ls(test_output_path)
parquet_files = [
    entry.path
    for entry in output_entries
    if entry.path.endswith(".parquet")
]

print(f"Parquet files generated: {len(parquet_files)}")
for parquet_file in parquet_files:
    print(parquet_file)

if not parquet_files:
    raise FileNotFoundError(
        f"No Parquet files found at {test_output_path}."
    )
