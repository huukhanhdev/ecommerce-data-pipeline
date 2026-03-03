-- stg_orders.sql
-- Clean and standardize orders from the staging raw table

WITH source AS (
    SELECT
        order_id,
        user_id,
        status,
        shipping_fee,
        payment_method,
        created_at,
        updated_at,
        _loaded_at,
        row_number() OVER (PARTITION BY order_id ORDER BY _loaded_at DESC) AS rn
    FROM {{ source('staging', 'stg_orders') }}
    WHERE order_id IS NOT NULL
      AND user_id IS NOT NULL
)

SELECT
    order_id,
    user_id,
    status,
    shipping_fee,
    payment_method,
    created_at,
    updated_at,
    _loaded_at
FROM source
WHERE rn = 1
