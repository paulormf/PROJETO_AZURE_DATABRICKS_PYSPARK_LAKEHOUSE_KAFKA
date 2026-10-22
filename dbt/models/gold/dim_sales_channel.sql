{{
    config(
        materialized='table',
        alias='dim_sales_channel',
        tags=['gold', 'dimension']
    )
}}

select
    sales_channel_id,
    channel_name,
    channel_type,
    status,
    current_timestamp() as _gold_processed_at
from {{ source('silver', 'sales_channels') }}
