-- dim_sellers.sql



SELECT
    seller_id                    AS seller_key,
    seller_id,
    name,
    city,
    rating,
    joined_at
FROM `staging`.`stg_sellers`
WHERE seller_id IS NOT NULL