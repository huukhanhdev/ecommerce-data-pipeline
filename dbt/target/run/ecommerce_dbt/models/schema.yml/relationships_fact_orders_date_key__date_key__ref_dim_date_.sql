
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select date_key as from_field
    from `staging_analytics`.`fact_orders`
    where date_key is not null
),

parent as (
    select date_key as to_field
    from `staging_analytics`.`dim_date`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null
-- end_of_sql
settings join_use_nulls = 1



  
  
    ) dbt_internal_test