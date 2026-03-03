
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select user_key
from `staging_analytics`.`fact_orders`
where user_key is null



  
  
    ) dbt_internal_test