
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select product_key
from `staging_analytics`.`dim_products`
where product_key is null



  
  
    ) dbt_internal_test