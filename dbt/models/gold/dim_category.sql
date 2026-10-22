{{
    config(
        materialized='table',
        alias='dim_category',
        tags=['gold', 'dimension']
    )
}}

select
    category_id,
    category_name,
    parent_category_id,
    status,
    created_at,
    updated_at,
    current_timestamp() as _gold_processed_at
from {{ source('silver', 'categories') }}
