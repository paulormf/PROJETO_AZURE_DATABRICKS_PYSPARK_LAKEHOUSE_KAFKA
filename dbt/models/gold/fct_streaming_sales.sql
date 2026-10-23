{{
    config(
        materialized='table',
        alias='fct_streaming_sales',
        tags=['gold', 'streaming', 'sales']
    )
}}

select
    cast(event_time as date) as event_date,
    sales_channel_id,
    product_id,
    count(distinct event_id) as event_count,
    count(distinct order_id) as order_count,
    sum(quantity) as item_quantity,
    sum(quantity * unit_price) as gross_sales,
    current_timestamp() as _gold_processed_at
from {{ source('silver', 'kinesis_sales_events') }}
group by
    cast(event_time as date),
    sales_channel_id,
    product_id
