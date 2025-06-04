-- src/main/resources/sql/ods_to_dwd/transform_users.sql
-- Example SQL for transforming raw user data to DWD user dimension.
-- Assumes a temporary view 'ods_users_raw_view' has been created from the ODS source.
-- Parameters like ${load_date} will be replaced by Scala code.

SELECT
    CAST(user_id AS BIGINT) AS user_id,
    TRIM(COALESCE(first_name, '')) AS first_name,
    TRIM(COALESCE(last_name, '')) AS last_name,
    LOWER(TRIM(email)) AS email,
    CASE
        WHEN gender IN ('Male', 'M', 'male') THEN 'M'
        WHEN gender IN ('Female', 'F', 'female') THEN 'F'
        ELSE 'U' -- Unknown
        END AS gender,
    TO_DATE(registration_ts) AS registration_date, -- Assuming registration_ts is a timestamp or string
    COALESCE(is_active, false) AS is_active,
    -- Add SCD Type 1/2 logic if needed (e.g., start_date, end_date, is_current)
    current_timestamp() AS dwd_insert_ts, -- Timestamp of DWD processing
    '${load_date}' AS etl_batch_date      -- ETL batch date for partitioning/tracking
FROM
    ods_users_raw_view
WHERE
    user_id IS NOT NULL AND email IS NOT NULL
-- Add more data quality checks as needed
-- e.g., AND email RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'
;