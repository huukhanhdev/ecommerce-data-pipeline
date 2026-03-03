-- dim_users.sql

{{ config(materialized='table') }}

SELECT
    user_id                      AS user_key,
    user_id,
    name,
    email,
    city,
    gender,
    created_at
FROM {{ ref('stg_users') }}
