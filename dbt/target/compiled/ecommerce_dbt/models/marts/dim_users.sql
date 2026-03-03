-- dim_users.sql



SELECT
    user_id                      AS user_key,
    user_id,
    name,
    email,
    city,
    gender,
    created_at
FROM `staging_staging`.`stg_users`