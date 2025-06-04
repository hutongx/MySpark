-- src/main/resources/sql/ods_to_dwd/transform_orders.sql
-- Example SQL for transforming raw orders data to DWD order fact.
-- Assumes a temporary view 'ods_orders_raw_view' has been created.

SELECT
    CAST(order_id AS STRING) AS order_id,
    CAST(user_id AS BIGINT) AS user_id,
    CAST(product_id AS STRING) AS product_id,
    CAST(order_quantity AS INT) AS order_quantity,
    CAST(order_amount AS DECIMAL(18, 2)) AS order_amount,
    TO_TIMESTAMP(order_timestamp) AS order_ts, -- Keep as timestamp for fact table
    -- Add any necessary cleaning or derivation for order status, payment method etc.
    COALESCE(LOWER(TRIM(order_status)), 'unknown') as order_status,
    current_timestamp() AS dwd_insert_ts,
    '${load_date}' AS etl_batch_date
FROM
    ods_orders_raw_view
WHERE
    order_id IS NOT NULL AND user_id IS NOT NULL AND product_id IS NOT NULL AND order_timestamp IS NOT NULL
  AND order_quantity > 0 AND order_amount >= 0
;