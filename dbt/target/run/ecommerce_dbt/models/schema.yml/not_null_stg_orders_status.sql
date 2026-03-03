
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select status
from `staging_staging`.`stg_orders`
where status is null



  
  
    ) dbt_internal_test