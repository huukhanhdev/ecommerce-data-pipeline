-- dim_sellers.sql

{{ config(materialized='table') }}

SELECT
    seller_id                    AS seller_key,
    seller_id,
    name,
    city,
    rating,
    joined_at
FROM {{ source('staging', 'stg_sellers') }}
WHERE seller_id IS NOT NULL
