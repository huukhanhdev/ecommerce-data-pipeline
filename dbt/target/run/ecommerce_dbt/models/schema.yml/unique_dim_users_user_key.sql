
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    user_key as unique_field,
    count(*) as n_records

from `staging_analytics`.`dim_users`
where user_key is not null
group by user_key
having count(*) > 1



  
  
    ) dbt_internal_test