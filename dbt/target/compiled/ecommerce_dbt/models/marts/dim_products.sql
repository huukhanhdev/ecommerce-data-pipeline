-- dim_products.sql



SELECT
    product_id                   AS product_key,
    product_id,
    seller_id,
    category_id,
    category_name,
    product_name                 AS name,
    base_price,
    is_active
FROM `staging_staging`.`stg_products`