

  create or replace view `staging_staging`.`stg_products` 
  
    
  
  
    
    
  as (
    -- stg_products.sql

SELECT
    p.product_id,
    p.seller_id,
    p.category_id,
    c.name AS category_name,
    p.name AS product_name,
    p.base_price,
    p.is_active
FROM `staging`.`stg_products` p
LEFT JOIN `staging`.`stg_categories` c
    ON p.category_id = c.category_id
WHERE p.product_id IS NOT NULL
    
  )
      
      
                    -- end_of_sql
                    
                    