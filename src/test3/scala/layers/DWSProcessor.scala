package layers

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

class DWSProcessor(spark: SparkSession) {
    import spark.implicits._
    private val logger = LoggerFactory.getLogger(this.getClass)

    def generateCustomerSummary(batchDate: String): Unit = {
        logger.info(s"Generating customer summary for $batchDate")

        val customerSummary = spark.sql(s"""
              SELECT
                age_group,
                customer_status_desc,
                COUNT(*) as customer_count,
                COUNT(CASE WHEN last_login_date >= date_sub('$batchDate', 30) THEN 1 END) as active_last_30_days,
                ROUND(
                  COUNT(CASE WHEN last_login_date >= date_sub('$batchDate', 30) THEN 1 END) * 100.0 / COUNT(*),
                  2
                ) as active_rate_pct,
                '$batchDate' as summary_date,
                current_timestamp() as etl_timestamp
              FROM dwd.dim_customer
              WHERE etl_date = '$batchDate'
              GROUP BY age_group, customer_status_desc
            """)

        customerSummary.write
          .mode("overwrite")
          .partitionBy("summary_date")
          .saveAsTable("dws.customer_summary")

        logger.info("Customer summary generated successfully")
    }

    def generateOrderStatistics(batchDate: String): Unit = {
        logger.info(s"Generating order statistics for $batchDate")

        // Daily order statistics
        val dailyOrderStats = spark.sql(s"""
              SELECT
                order_date,
                COUNT(*) as total_orders,
                SUM(order_amount) as total_revenue,
                AVG(order_amount) as avg_order_value,
                MIN(order_amount) as min_order_value,
                MAX(order_amount) as max_order_value,
                STDDEV(order_amount) as stddev_order_value,
                COUNT(DISTINCT customer_id) as unique_customers,
                '$batchDate' as etl_date,
                current_timestamp() as etl_timestamp
              FROM dwd.fact_order
              WHERE etl_date = '$batchDate'
              GROUP BY order_date
            """)

        dailyOrderStats.write
          .mode("overwrite")
          .partitionBy("etl_date")
          .saveAsTable("dws.daily_order_stats")

        // Customer behavior analysis
        val customerBehaviorStats = spark.sql(s"""
              SELECT
                c.age_group,
                o.order_size_category,
                COUNT(*) as order_count,
                SUM(o.order_amount) as total_spent,
                AVG(o.order_amount) as avg_order_amount,
                PERCENTILE_APPROX(o.order_amount, 0.5) as median_order_amount,
                PERCENTILE_APPROX(o.order_amount, 0.9) as p90_order_amount,
                '$batchDate' as analysis_date,
                current_timestamp() as etl_timestamp
              FROM dwd.fact_order o
              JOIN dwd.dim_customer c ON o.customer_id = c.customer_id
              WHERE o.etl_date = '$batchDate'
              GROUP BY c.age_group, o.order_size_category
            """)

        customerBehaviorStats.write
          .mode("overwrite")
          .partitionBy("analysis_date")
          .saveAsTable("dws.customer_behavior_stats")

        logger.info("Order statistics generated successfully")
    }
}
