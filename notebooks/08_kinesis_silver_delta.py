# Databricks notebook source
"""Transforma eventos de vendas do Kinesis Bronze em Silver Delta."""

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


# COMMAND ----------

dbutils.widgets.text(
    "bronze_path",
    "s3://lakehouse-prj-db-kafka/lakehouse/bronze/kinesis/sales-events/",
)
dbutils.widgets.text(
    "silver_path",
    "s3://lakehouse-prj-db-kafka/lakehouse/silver/kinesis/sales-events/",
)

bronze_path = dbutils.widgets.get("bronze_path")
silver_path = dbutils.widgets.get("silver_path")

print(f"Bronze path: {bronze_path}")
print(f"Silver path: {silver_path}")


# COMMAND ----------

event_schema = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("order_id", LongType(), True),
        StructField("sales_channel_id", LongType(), True),
        StructField("customer_id", LongType(), True),
        StructField(
            "items",
            ArrayType(
                StructType(
                    [
                        StructField("product_id", LongType(), True),
                        StructField("quantity", IntegerType(), True),
                        StructField("unit_price", DecimalType(18, 2), True),
                    ]
                )
            ),
            True,
        ),
    ]
)

df_bronze = spark.read.parquet(bronze_path)
bronze_rows = df_bronze.count()
print(f"Eventos lidos da Bronze: {bronze_rows}")

if bronze_rows == 0:
    dbutils.notebook.exit("NO_NEW_DATA")

df_events = (
    df_bronze
    .withColumn("event", F.from_json(F.col("event_json"), event_schema))
    .select(
        F.col("event.event_id").alias("event_id"),
        F.col("event.event_type").alias("event_type"),
        F.col("event.event_time").alias("event_time"),
        F.col("event.order_id").alias("order_id"),
        F.col("event.sales_channel_id").alias("sales_channel_id"),
        F.col("event.customer_id").alias("customer_id"),
        F.col("event.items").alias("items"),
        "event_json",
        F.col("partitionKey").alias("partition_key"),
        F.col("sequenceNumber").alias("sequence_number"),
        "_ingested_at",
        "_source",
    )
)

df_silver = (
    df_events
    .select("*", F.posexplode_outer("items").alias("item_position", "item"))
    .select(
        "event_id",
        "event_type",
        F.to_timestamp("event_time").alias("event_time"),
        "order_id",
        "sales_channel_id",
        "customer_id",
        "item_position",
        F.col("item.product_id").alias("product_id"),
        F.col("item.quantity").alias("quantity"),
        F.col("item.unit_price").alias("unit_price"),
        "partition_key",
        "sequence_number",
        "_ingested_at",
        "_source",
        "event_json",
    )
    .withColumn("_silver_processed_at", F.current_timestamp())
    .filter(F.col("event_id").isNotNull())
    .filter(F.col("item_position").isNotNull())
    .dropDuplicates(["event_id", "item_position"])
)

silver_rows = df_silver.count()
print(f"Linhas Silver após normalização: {silver_rows}")


# COMMAND ----------

def path_exists(path: str) -> bool:
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False


if not path_exists(silver_path):
    (
        df_silver.write
        .format("delta")
        .mode("overwrite")
        .save(silver_path)
    )
    print("Tabela Silver Delta criada.")
else:
    from delta.tables import DeltaTable

    silver_table = DeltaTable.forPath(spark, silver_path)
    (
        silver_table.alias("target")
        .merge(
            df_silver.alias("source"),
            "target.event_id = source.event_id "
            "AND target.item_position = source.item_position",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print("Tabela Silver Delta atualizada com MERGE.")

validated_rows = spark.read.format("delta").load(silver_path).count()
print(f"Linhas Silver Delta após gravação: {validated_rows}")
print(f"Silver path: {silver_path}")
