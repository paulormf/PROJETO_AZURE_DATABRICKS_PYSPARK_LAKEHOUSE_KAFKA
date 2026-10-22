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
    from {{ ref('dim_product') }}
),

categories as (
    select category_id, category_name
    from {{ ref('dim_category') }}
),

brands as (
    select brand_id, brand_name
    from {{ ref('dim_brand') }}
),

sales_channels as (
    select sales_channel_id, channel_name
    from {{ ref('dim_sales_channel') }}
),

sales as (
    select
        orders.order_id,
        orders.sales_channel_id,
        sales_channels.channel_name,
        orders.order_date,
        orders.status,
        products.category_id,
        categories.category_name,
        products.brand_id,
        brands.brand_name,
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
    left join categories
        on products.category_id = categories.category_id
    left join brands
        on products.brand_id = brands.brand_id
    left join sales_channels
        on orders.sales_channel_id = sales_channels.sales_channel_id
)

select
    order_date,
    sales_channel_id,
    channel_name,
    category_id,
    category_name,
    brand_id,
    brand_name,
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
    channel_name,
    category_id,
    category_name,
    brand_id,
    brand_name,
    status
