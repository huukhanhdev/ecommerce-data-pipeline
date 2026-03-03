-- stg_users.sql

SELECT
    user_id,
    name,
    email,
    phone,
    city,
    gender,
    created_at
FROM {{ source('staging', 'stg_users') }}
WHERE user_id IS NOT NULL
  AND email IS NOT NULL
