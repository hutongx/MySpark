-- src/main/resources/sql/dwd_to_dws/aggregate_daily_sales.sql
-- Example SQL for creating a daily sales summary DWS table.
-- Assumes DWD views 'dwd_fact_orders_view' and 'dwd_dim_products_view' (if needed) are available.
-- Processes data for a specific ${load_date} from DWD.

SELECT
    DATE(fo.order_ts) AS sales_date, -- or fo.etl_batch_date if orders are daily snapshots
    -- dp.product_category, -- Assuming you join with a product dimension loaded from DWD
    -- dp.product_brand,
    SUM(fo.order_amount) AS total_sales_amount,
    COUNT(DISTINCT fo.order_id) AS total_orders_count,
    SUM(fo.order_quantity) AS total_items_sold,
    AVG(fo.order_amount) AS average_order_value,
    current_timestamp() AS dws_insert_ts,
    '${load_date}' AS etl_batch_date -- Partitioning key for the DWS table
FROM
    dwd_fact_orders_view fo
-- LEFT JOIN
-- dwd_dim_products_view dp ON fo.product_id = dp.product_id AND dp.etl_batch_date = '${load_date}' -- Join on same batch
WHERE
    fo.etl_batch_date = '${load_date}' -- Process only the DWD data from the current batch
-- Add any other filtering conditions, e.g., for specific order statuses
  AND fo.order_status NOT IN ('cancelled', 'returned_full')
GROUP BY
    DATE(fo.order_ts) -- Or fo.etl_batch_date
-- dp.product_category,
-- dp.product_brand
;