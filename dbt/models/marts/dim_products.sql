-- dim_products.sql

{{ config(materialized='table') }}

SELECT
    product_id                   AS product_key,
    product_id,
    seller_id,
    category_id,
    category_name,
    product_name                 AS name,
    base_price,
    is_active
FROM {{ ref('stg_products') }}
