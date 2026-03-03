-- rpt_product_performance.sql
-- Product sales performance with cumulative revenue and ranking

{{ config(materialized='table') }}

WITH product_sales AS (
    SELECT
        f.product_key,
        p.name                                    AS product_name,
        p.category_name,
        p.seller_id,

        countDistinct(f.order_id)                 AS total_orders,
        sum(f.quantity)                           AS total_units_sold,
        sum(f.total_amount)                       AS total_revenue,
        avg(f.unit_price)                         AS avg_selling_price,
        p.base_price,
        avg(f.unit_price) - p.base_price          AS avg_discount_amount,
        countIf(f.status = 'returned')            AS returns
    FROM {{ ref('fact_orders') }}   f
    JOIN {{ ref('dim_products') }}  p ON f.product_key = p.product_key
    WHERE f.status != 'cancelled'
    GROUP BY
        f.product_key, p.name, p.category_name, p.seller_id, p.base_price
)
SELECT
    *,
    -- Rank product within its category by revenue
    RANK() OVER (
        PARTITION BY category_name
        ORDER BY total_revenue DESC
    )                                             AS revenue_rank_in_category,

    -- Cumulative revenue within category (sorted by revenue desc)
    SUM(total_revenue) OVER (
        PARTITION BY category_name
        ORDER BY total_revenue DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                             AS cumulative_revenue_in_category,

    round(returns * 100.0 / nullIf(total_orders, 0), 2) AS return_rate_pct
FROM product_sales
ORDER BY total_revenue DESC
