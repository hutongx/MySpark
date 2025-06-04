-- Assume ODS ods_users_raw table/view exists

-- Clean user data: handle nulls, cast types, filter invalid records
SELECT
    CAST(user_id AS BIGINT) as user_id,
    COALESCE(LOWER(TRIM(username)), 'unknown') AS username,
    LOWER(TRIM(email)) AS email,
    CASE
        WHEN gender IN ('Male', 'male', 'M') THEN 'M'
        WHEN gender IN ('Female', 'female', 'F') THEN 'F'
        ELSE 'U' -- Unknown
        END AS gender,
    CAST(registration_date AS DATE) as registration_date,
    COALESCE(is_active, false) AS is_active,
    current_timestamp() as dwd_processed_ts,
    '${load_date}' as etl_load_date -- Parameter for load date partitioning
FROM
    ods_users_raw
WHERE
    user_id IS NOT NULL AND email IS NOT NULL AND email LIKE '%@%'; -- Basic validation