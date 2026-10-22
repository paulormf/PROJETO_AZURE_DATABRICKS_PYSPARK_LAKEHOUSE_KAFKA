{{
    config(
        materialized='table',
        alias='dim_brand',
        tags=['gold', 'dimension']
    )
}}

select
    brand_id,
    brand_name,
    manufacturer,
    status,
    created_at,
    updated_at,
    current_timestamp() as _gold_processed_at
from {{ source('silver', 'brands') }}
