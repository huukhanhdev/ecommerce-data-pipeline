-- fact_orders.sql
-- Grain: one row per order_item
-- Joins staging tables to build a fully denormalized fact table



SELECT
    oi.item_id                                      AS order_item_key,
    oi.item_id,
    oi.order_id                                    AS order_id,
    o.user_id                                       AS user_key,
    oi.product_id                                   AS product_key,
    p.seller_id                                     AS seller_key,
    toInt32(formatDateTime(o.created_at, '%Y%m%d')) AS date_key,
    oi.quantity,
    oi.unit_price,
    o.shipping_fee,
    oi.quantity * oi.unit_price                     AS total_amount,
    o.status,
    o.payment_method,
    o.created_at                                    AS order_created_at
FROM `staging_staging`.`stg_order_items`   oi
JOIN `staging_staging`.`stg_orders`        o  ON oi.order_id  = o.order_id
LEFT JOIN `staging_staging`.`stg_products` p  ON oi.product_id = p.product_id
LEFT JOIN `staging_analytics`.`dim_date`     d  ON toInt32(formatDateTime(o.created_at, '%Y%m%d')) = d.date_key