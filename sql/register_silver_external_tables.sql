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

CREATE TABLE IF NOT EXISTS main.silver.customers
USING DELTA
LOCATION 's3://lakehouse-prj-db-kafka/lakehouse/silver/azure_sql/dbo/customers/';

CREATE TABLE IF NOT EXISTS main.silver.customer_contacts
USING DELTA
LOCATION 's3://lakehouse-prj-db-kafka/lakehouse/silver/azure_sql/dbo/customer_contacts/';

CREATE TABLE IF NOT EXISTS main.silver.customer_addresses
USING DELTA
LOCATION 's3://lakehouse-prj-db-kafka/lakehouse/silver/azure_sql/dbo/customer_addresses/';

CREATE TABLE IF NOT EXISTS main.silver.employees
USING DELTA
LOCATION 's3://lakehouse-prj-db-kafka/lakehouse/silver/azure_sql/dbo/employees/';

CREATE TABLE IF NOT EXISTS main.silver.suppliers
USING DELTA
LOCATION 's3://lakehouse-prj-db-kafka/lakehouse/silver/azure_sql/dbo/suppliers/';

CREATE TABLE IF NOT EXISTS main.silver.kinesis_sales_events
USING DELTA
LOCATION 's3://lakehouse-prj-db-kafka/lakehouse/silver/kinesis/sales-events/';

CREATE SCHEMA IF NOT EXISTS main.governed;

CREATE OR REPLACE VIEW main.governed.v_customers_analytics AS
SELECT
    customer_id,
    customer_type,
    YEAR(birth_date) AS birth_year,
    gender,
    status,
    created_at,
    updated_at
FROM main.silver.customers;

CREATE OR REPLACE VIEW main.governed.v_customer_contacts_analytics AS
SELECT
    customer_id,
    contact_type,
    is_primary,
    created_at,
    updated_at
FROM main.silver.customer_contacts;

CREATE OR REPLACE VIEW main.governed.v_customer_addresses_analytics AS
SELECT
    customer_id,
    address_type,
    city,
    state,
    country,
    is_primary,
    created_at,
    updated_at
FROM main.silver.customer_addresses;

CREATE OR REPLACE VIEW main.governed.v_employees_analytics AS
SELECT
    employee_id,
    department_id,
    manager_id,
    hire_date,
    termination_date,
    CASE
        WHEN salary IS NULL THEN 'UNKNOWN'
        WHEN salary < 3000 THEN 'A'
        WHEN salary < 7000 THEN 'B'
        WHEN salary < 15000 THEN 'C'
        ELSE 'D'
    END AS salary_band,
    status,
    created_at,
    updated_at
FROM main.silver.employees;

CREATE OR REPLACE VIEW main.governed.v_suppliers_analytics AS
SELECT
    supplier_id,
    company_name,
    trade_name,
    city,
    state,
    status,
    created_at,
    updated_at
FROM main.silver.suppliers;
