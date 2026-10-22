{{
    config(
        materialized='table',
        alias='fct_sales_daily',
        tags=['gold', 'sales']
    )
}}

with orders as (
    select
        order_id,
        sales_channel_id,
        cast(order_date as date) as order_date,
        status
    from {{ source('silver', 'orders') }}
),

order_items as (
    select
        order_id,
        product_id,
        coalesce(quantity, 0) as quantity,
        coalesce(unit_price, 0) as unit_price,
        coalesce(discount, 0) as discount,
        coalesce(tax, 0) as tax,
        coalesce(total_item, 0) as total_item
    from {{ source('silver', 'order_items') }}
),

products as (
    select
        product_id,
        category_id,
        brand_id
    from {{ source('silver', 'products') }}
),

sales as (
    select
        orders.order_id,
        orders.sales_channel_id,
        orders.order_date,
        orders.status,
        products.category_id,
        products.brand_id,
        order_items.quantity,
        order_items.unit_price,
        order_items.discount,
        order_items.tax,
        order_items.total_item
    from order_items
    inner join orders
        on order_items.order_id = orders.order_id
    left join products
        on order_items.product_id = products.product_id
)

select
    order_date,
    sales_channel_id,
    category_id,
    brand_id,
    status,
    count(distinct order_id) as order_count,
    sum(quantity) as item_quantity,
    sum(quantity * unit_price) as gross_sales,
    sum(discount) as discount_amount,
    sum(tax) as tax_amount,
    sum(total_item) as net_sales,
    current_timestamp() as _gold_processed_at
from sales
group by
    order_date,
    sales_channel_id,
    category_id,
    brand_id,
    status
