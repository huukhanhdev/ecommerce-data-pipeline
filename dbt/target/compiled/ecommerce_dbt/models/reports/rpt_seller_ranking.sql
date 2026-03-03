-- rpt_seller_ranking.sql
-- Seller performance ranking within each category using window functions



WITH seller_stats AS (
    SELECT
        f.seller_key,
        s.name                               AS seller_name,
        s.city                               AS seller_city,
        s.rating                             AS seller_rating,
        p.category_name,

        countDistinct(f.order_id)            AS total_orders,
        sum(f.quantity)                      AS total_units_sold,
        sum(f.total_amount)                  AS total_revenue,
        avg(f.total_amount)                  AS avg_order_value,
        countIf(f.status = 'returned')       AS returned_orders,
        countIf(f.status = 'cancelled')      AS cancelled_orders
    FROM `staging_analytics`.`fact_orders`   f
    JOIN `staging_analytics`.`dim_sellers`   s ON f.seller_key  = s.seller_key
    JOIN `staging_analytics`.`dim_products`  p ON f.product_key = p.product_key
    GROUP BY
        f.seller_key, s.name, s.city, s.rating, p.category_name
)
SELECT
    *,
    -- Rank by revenue within each category
    RANK() OVER (
        PARTITION BY category_name
        ORDER BY total_revenue DESC
    )                                        AS revenue_rank_in_category,

    -- Percentile within all sellers (across categories)
    percent_rank() OVER (
        ORDER BY total_revenue DESC
    )                                        AS revenue_percentile,

    round(
        returned_orders * 100.0 / nullIf(total_orders, 0), 2
    )                                        AS return_rate_pct
FROM seller_stats