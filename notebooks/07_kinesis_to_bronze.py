# Databricks notebook source
"""Lê eventos do Kinesis e grava o payload original na camada Bronze em Parquet."""

from pyspark.sql import functions as F


# COMMAND ----------

dbutils.widgets.text("stream_name", "lakehouse-sales-events")
dbutils.widgets.text("aws_region", "us-east-1")
dbutils.widgets.text(
    "bronze_path",
    "s3://lakehouse-prj-db-kafka/lakehouse/bronze/kinesis/sales-events/",
)
dbutils.widgets.text(
    "checkpoint_path",
    "s3://lakehouse-prj-db-kafka/lakehouse/checkpoints/kinesis/sales-events/",
)
dbutils.widgets.text("initial_position", "trim_horizon")

stream_name = dbutils.widgets.get("stream_name")
aws_region = dbutils.widgets.get("aws_region")
bronze_path = dbutils.widgets.get("bronze_path")
checkpoint_path = dbutils.widgets.get("checkpoint_path")
initial_position = dbutils.widgets.get("initial_position").lower()

print(f"Stream: {stream_name}")
print(f"Região: {aws_region}")
print(f"Bronze path: {bronze_path}")
print(f"Checkpoint path: {checkpoint_path}")
print(f"Posição inicial: {initial_position}")


# COMMAND ----------

aws_access_key = dbutils.secrets.get(scope="aws-kinesis", key="access-key")
aws_secret_key = dbutils.secrets.get(scope="aws-kinesis", key="secret-key")

df_kinesis = (
    spark.readStream.format("kinesis")
    .option("streamName", stream_name)
    .option("region", aws_region)
    .option("awsAccessKey", aws_access_key)
    .option("awsSecretKey", aws_secret_key)
    .option("initialPosition", initial_position)
    .option("consumerMode", "polling")
    .option("failOnDataLoss", "true")
    .load()
)


# COMMAND ----------

df_bronze = (
    df_kinesis.withColumn("event_json", F.col("data").cast("string"))
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source", F.lit("kinesis"))
    .drop("data")
)


# COMMAND ----------

query = (
    df_bronze.writeStream.format("parquet")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .start(bronze_path)
)

query.awaitTermination()

progress = query.lastProgress
if progress:
    print(f"Batch ID: {progress.get('batchId')}")
    print(f"Registros processados: {progress.get('numInputRows')}")
    for source in progress.get("sources", []):
        print(f"Métricas da fonte: {source.get('metrics', {})}")
else:
    print("Nenhum evento novo foi encontrado no Kinesis.")

print(f"Eventos gravados em Parquet: {bronze_path}")
