{{
    config(
        materialized='table',
        alias='dim_product',
        tags=['gold', 'dimension']
    )
}}

select
    product_id,
    sku,
    product_name,
    category_id,
    brand_id,
    supplier_id,
    unit,
    weight,
    cost_price,
    sale_price,
    tax_rate,
    status,
    created_at,
    updated_at,
    current_timestamp() as _gold_processed_at
from {{ source('silver', 'products') }}
