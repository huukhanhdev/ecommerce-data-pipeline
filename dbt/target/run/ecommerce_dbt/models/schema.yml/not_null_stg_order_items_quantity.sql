
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select quantity
from `staging_staging`.`stg_order_items`
where quantity is null



  
  
    ) dbt_internal_test