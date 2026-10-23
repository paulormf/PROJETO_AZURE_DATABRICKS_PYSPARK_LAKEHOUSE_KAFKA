# Databricks notebook source
"""Lê eventos do Kinesis e grava o payload original na camada Bronze em Parquet."""

from pyspark.sql import functions as F


# COMMAND ----------

dbutils.widgets.text(
    "stream_arn",
    "arn:aws:kinesis:us-east-1:693000032696:stream/lakehouse-sales-events",
)
dbutils.widgets.text("aws_region", "us-east-1")
dbutils.widgets.text(
    "bronze_path",
    "s3://lakehouse-prj-db-kafka/lakehouse/bronze/kinesis/sales-events/",
)
dbutils.widgets.text(
    "checkpoint_path",
    "s3://lakehouse-prj-db-kafka/lakehouse/checkpoints/kinesis/sales-events/",
)
dbutils.widgets.text("initial_position", "earliest")

stream_arn = dbutils.widgets.get("stream_arn")
aws_region = dbutils.widgets.get("aws_region")
bronze_path = dbutils.widgets.get("bronze_path")
checkpoint_path = dbutils.widgets.get("checkpoint_path")
initial_position = dbutils.widgets.get("initial_position").lower()

print(f"Stream ARN: {stream_arn}")
print(f"Região: {aws_region}")
print(f"Bronze path: {bronze_path}")
print(f"Checkpoint path: {checkpoint_path}")
print(f"Posição inicial: {initial_position}")
print("Trigger: availableNow")


# COMMAND ----------

aws_access_key = dbutils.secrets.get(scope="aws-kinesis", key="access-key")
aws_secret_key = dbutils.secrets.get(scope="aws-kinesis", key="secret-key")

df_kinesis = (
    spark.readStream.format("kinesis")
    .option("streamARN", stream_arn)
    .option("region", aws_region)
    .option("awsAccessKey", aws_access_key)
    .option("awsSecretKey", aws_secret_key)
    .option("initialPosition", initial_position)
    .option("consumerMode", "polling")
    .option("maxFetchDuration", "60s")
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

progress_events = list(query.recentProgress)
if progress_events:
    total_records = 0
    print(f"Micro-lotes executados: {len(progress_events)}")

    for progress in progress_events:
        batch_id = progress.get("batchId")
        batch_records = 0

        for source in progress.get("sources", []):
            metrics = source.get("metrics", {})
            source_records = int(metrics.get("numProcessedRecords", 0) or 0)
            batch_records += source_records
            print(f"Batch ID: {batch_id}")
            print(f"Registros no batch: {source_records}")
            print(f"Métricas da fonte: {metrics}")

        total_records += batch_records

    print(f"Total de eventos processados: {total_records}")
else:
    print("Nenhum evento novo foi encontrado no Kinesis.")

print(f"Eventos gravados em Parquet: {bronze_path}")
