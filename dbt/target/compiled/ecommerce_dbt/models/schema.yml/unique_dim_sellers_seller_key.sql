
    
    

select
    seller_key as unique_field,
    count(*) as n_records

from `staging_analytics`.`dim_sellers`
where seller_key is not null
group by seller_key
having count(*) > 1


