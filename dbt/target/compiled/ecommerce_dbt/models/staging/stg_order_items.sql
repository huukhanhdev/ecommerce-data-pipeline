-- stg_order_items.sql

WITH source AS (
    SELECT
        item_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        quantity * unit_price AS line_total,
        _loaded_at,
        row_number() OVER (PARTITION BY item_id ORDER BY _loaded_at DESC) AS rn
    FROM `staging`.`stg_order_items`
    WHERE item_id   IS NOT NULL
      AND order_id  IS NOT NULL
      AND quantity  > 0
      AND unit_price > 0
)

SELECT
    item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    line_total,
    _loaded_at
FROM source
WHERE rn = 1