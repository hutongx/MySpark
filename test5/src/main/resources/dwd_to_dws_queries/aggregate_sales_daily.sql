-- Assume DWD tables dwd_fact_orders and dwd_dim_products exist

-- Aggregate daily sales
-- This is a simplified example. Real DWS tables might involve more complex joins and logic.
SELECT
    fo.order_date,
    dp.product_category,
    SUM(fo.order_amount) as total_sales_amount,
    COUNT(DISTINCT fo.order_id) as total_orders,
    current_timestamp() as dws_processed_ts,
    '${load_date}' as etl_load_date -- For partitioning the DWS table
FROM
    dwd_fact_orders fo
        JOIN
    dwd_dim_products dp ON fo.product_id = dp.product_id
WHERE
        fo.etl_load_date = '${load_date}' -- Process data from DWD loaded on this date
  AND dp.etl_load_date = '${load_date}' -- Assuming dim also has load date or is SCD2 and you pick correct version
GROUP BY
    fo.order_date,
    dp.product_category;