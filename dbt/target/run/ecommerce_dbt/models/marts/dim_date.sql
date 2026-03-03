
  
    
    
    
        
         


        
  

  insert into `staging_analytics`.`dim_date__dbt_backup`
        ("date_key", "full_date", "day", "month", "quarter", "year", "week_of_year", "day_name", "month_name", "is_weekend")-- dim_date.sql (updated: adds week_of_year column for rpt_daily_revenue)



WITH date_spine AS (
    SELECT addDays(toDate('2023-01-01'), number) AS full_date
    FROM numbers(dateDiff('day', toDate('2023-01-01'), toDate('2027-12-31')) + 1)
)
SELECT
    toInt32(formatDateTime(full_date, '%Y%m%d')) AS date_key,
    full_date,
    toDayOfMonth(full_date)                       AS day,
    toMonth(full_date)                            AS month,
    toQuarter(full_date)                          AS quarter,
    toYear(full_date)                             AS year,
    toISOWeek(full_date)                          AS week_of_year,
    arrayElement(
        ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'],
        toDayOfWeek(full_date)
    )                                             AS day_name,
    arrayElement(
        ['January','February','March','April','May','June','July','August','September','October','November','December'],
        toMonth(full_date)
    )                                             AS month_name,
    toDayOfWeek(full_date) IN (6, 7)              AS is_weekend
FROM date_spine
  