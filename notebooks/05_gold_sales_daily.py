# Databricks notebook source

# COMMAND ----------

from pyspark.sql import functions as F


silver_root = "s3://lakehouse-prj-db-kafka/lakehouse/silver/azure_sql/dbo/"
gold_path = (
    "s3://lakehouse-prj-db-kafka/"
    "lakehouse/gold/business/sales_daily/"
)

orders = spark.read.format("delta").load(f"{silver_root}orders/")
order_items = spark.read.format("delta").load(f"{silver_root}order_items/")
products = spark.read.format("delta").load(f"{silver_root}products/")

required_columns = {
    "orders": {
        "order_id",
        "sales_channel_id",
        "order_date",
        "status",
    },
    "order_items": {
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "discount",
        "tax",
        "total_item",
    },
    "products": {"product_id", "category_id", "brand_id"},
}

for dataset_name, columns in {
    "orders": orders,
    "order_items": order_items,
    "products": products,
}.items():
    missing = required_columns[dataset_name] - set(columns.columns)
    if missing:
        raise ValueError(
            f"Missing columns in {dataset_name}: {sorted(missing)}"
        )

print(f"Orders rows: {orders.count()}")
print(f"Order items rows: {order_items.count()}")
print(f"Products rows: {products.count()}")

# Customer identifiers are intentionally not published in this Gold table.
sales = (
    order_items.alias("item")
    .join(
        orders.select(
            "order_id",
            "sales_channel_id",
            "order_date",
            "status",
        ).alias("order"),
        on="order_id",
        how="inner",
    )
    .join(
        products.select("product_id", "category_id", "brand_id").alias("product"),
        on="product_id",
        how="left",
    )
)

gold_sales_daily = (
    sales
    .withColumn("order_date", F.to_date("order_date"))
    .withColumn("quantity", F.coalesce(F.col("quantity"), F.lit(0)))
    .withColumn("unit_price", F.coalesce(F.col("unit_price"), F.lit(0)))
    .withColumn("discount", F.coalesce(F.col("discount"), F.lit(0)))
    .withColumn("tax", F.coalesce(F.col("tax"), F.lit(0)))
    .withColumn("total_item", F.coalesce(F.col("total_item"), F.lit(0)))
    .groupBy(
        "order_date",
        "sales_channel_id",
        "category_id",
        "brand_id",
        "status",
    )
    .agg(
        F.countDistinct("order_id").alias("order_count"),
        F.sum("quantity").alias("item_quantity"),
        F.sum(F.col("quantity") * F.col("unit_price")).alias("gross_sales"),
        F.sum("discount").alias("discount_amount"),
        F.sum("tax").alias("tax_amount"),
        F.sum("total_item").alias("net_sales"),
    )
    .withColumn("_gold_processed_at", F.current_timestamp())
)

gold_rows = gold_sales_daily.count()
print(f"Gold daily sales rows: {gold_rows}")

(
    gold_sales_daily.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(gold_path)
)

validated_rows = spark.read.format("delta").load(gold_path).count()
print(f"Gold rows written: {validated_rows}")
print(f"Gold path: {gold_path}")

if validated_rows != gold_rows:
    raise RuntimeError(
        f"Gold validation failed: expected {gold_rows}, "
        f"found {validated_rows}."
    )

display(
    spark.read.format("delta").load(gold_path)
    .orderBy(F.col("order_date").desc())
    .limit(20)
)
