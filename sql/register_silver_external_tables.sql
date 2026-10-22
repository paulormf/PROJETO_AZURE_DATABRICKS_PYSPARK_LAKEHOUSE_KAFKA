CREATE SCHEMA IF NOT EXISTS main.silver;

CREATE TABLE IF NOT EXISTS main.silver.orders
USING DELTA
LOCATION 's3://lakehouse-prj-db-kafka/lakehouse/silver/azure_sql/dbo/orders/';

CREATE TABLE IF NOT EXISTS main.silver.order_items
USING DELTA
LOCATION 's3://lakehouse-prj-db-kafka/lakehouse/silver/azure_sql/dbo/order_items/';

CREATE TABLE IF NOT EXISTS main.silver.products
USING DELTA
LOCATION 's3://lakehouse-prj-db-kafka/lakehouse/silver/azure_sql/dbo/products/';

CREATE TABLE IF NOT EXISTS main.silver.categories
USING DELTA
LOCATION 's3://lakehouse-prj-db-kafka/lakehouse/silver/azure_sql/dbo/categories/';

CREATE TABLE IF NOT EXISTS main.silver.brands
USING DELTA
LOCATION 's3://lakehouse-prj-db-kafka/lakehouse/silver/azure_sql/dbo/brands/';

CREATE TABLE IF NOT EXISTS main.silver.sales_channels
USING DELTA
LOCATION 's3://lakehouse-prj-db-kafka/lakehouse/silver/azure_sql/dbo/sales_channels/';
