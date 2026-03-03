
  
    
    
    
        
         


        
  

  insert into `staging_analytics`.`rpt_daily_revenue__dbt_backup`
        ("full_date", "year", "month", "quarter", "day_name", "is_weekend", "week_of_year", "category_name", "status", "payment_method", "total_orders", "total_items_sold", "gross_revenue", "total_shipping_fees", "avg_order_value", "revenue_7d_rolling_avg", "daily_rank_in_week")-- rpt_daily_revenue.sql
-- Daily revenue breakdown by category, status, and payment method



WITH daily_rev AS (
    SELECT
        d.full_date,
        d.year,
        d.month,
        d.quarter,
        d.day_name,
        d.is_weekend,
        d.week_of_year,
        p.category_name,
        f.status,
        f.payment_method,
        countDistinct(f.order_id) AS total_orders,
        sum(f.quantity)           AS total_items_sold,
        sum(f.total_amount)       AS gross_revenue,
        sum(f.shipping_fee)       AS total_shipping_fees,
        avg(f.total_amount)       AS avg_order_value
    FROM `staging_analytics`.`fact_orders` f
    JOIN `staging_analytics`.`dim_date`     d ON f.date_key    = d.date_key
    JOIN `staging_analytics`.`dim_products` p ON f.product_key = p.product_key
    WHERE f.status != 'cancelled'
    GROUP BY
        d.full_date, d.year, d.month, d.quarter, d.day_name, d.is_weekend,
        d.week_of_year, p.category_name, f.status, f.payment_method
)
SELECT
    *,
    avg(gross_revenue) OVER (
        PARTITION BY category_name
        ORDER BY full_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS revenue_7d_rolling_avg,
    rank() OVER (
        PARTITION BY year, week_of_year
        ORDER BY gross_revenue DESC
    ) AS daily_rank_in_week
FROM daily_rev
  