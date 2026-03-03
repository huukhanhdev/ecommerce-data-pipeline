

  create or replace view `staging_staging`.`stg_users` 
  
    
  
  
    
    
  as (
    -- stg_users.sql

SELECT
    user_id,
    name,
    email,
    phone,
    city,
    gender,
    created_at
FROM `staging`.`stg_users`
WHERE user_id IS NOT NULL
  AND email IS NOT NULL
    
  )
      
      
                    -- end_of_sql
                    
                    