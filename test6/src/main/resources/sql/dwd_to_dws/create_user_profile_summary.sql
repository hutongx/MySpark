-- src/main/resources/sql/dwd_to_dws/create_user_profile_summary.sql
-- Example: Create a user summary table (wide table)
-- Combines user dimension with aggregated facts.
SELECT
    du.user_id,
    du.first_name,
    du.last_name,
    du.email,
    du.gender,
    du.registration_date,
    MAX(fo.order_ts) AS last_order_timestamp,
    SUM(fo.order_amount) AS total_lifetime_spend,
    COUNT(DISTINCT fo.order_id) AS total_lifetime_orders,
    current_timestamp() AS dws_insert_ts,
    '${load_date}' AS etl_batch_date -- This DWS table might be a full rebuild or incremental
FROM
    dwd_cleaned_users_view du
        LEFT JOIN
    dwd_fact_orders_view fo ON du.user_id = fo.user_id
        AND fo.etl_batch_date <= '${load_date}' -- Consider all historical orders for lifetime summary
        AND du.etl_batch_date = '${load_date}' -- Use current version of user dimension
WHERE
        du.etl_batch_date = '${load_date}' -- Process current batch of users
GROUP BY
    du.user_id,
    du.first_name,
    du.last_name,
    du.email,
    du.gender,
    du.registration_date
;