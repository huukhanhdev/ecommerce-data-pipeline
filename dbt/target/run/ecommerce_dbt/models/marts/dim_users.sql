
  
    
    
    
        
         


        
  

  insert into `staging_analytics`.`dim_users__dbt_backup`
        ("user_key", "user_id", "name", "email", "city", "gender", "created_at")-- dim_users.sql



SELECT
    user_id                      AS user_key,
    user_id,
    name,
    email,
    city,
    gender,
    created_at
FROM `staging_staging`.`stg_users`
  