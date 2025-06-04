package layers

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

class DWDProcessor(spark: SparkSession) {
    import spark.implicits._
    private val logger = LoggerFactory.getLogger(this.getClass)

    def processCustomerDWD(batchDate: String): Unit = {
        logger.info(s"Processing customer DWD data for $batchDate")

        val customerDWD = spark.sql(s"""
              SELECT
                customer_id,
                customer_name,
                CASE
                  WHEN age < 18 THEN 'Minor'
                  WHEN age BETWEEN 18 AND 35 THEN 'Young Adult'
                  WHEN age BETWEEN 36 AND 50 THEN 'Middle Age'
                  ELSE 'Senior'
                END as age_group,
                email,
                phone,
                address,
                city,
                state,
                CASE
                  WHEN customer_status = 'A' THEN 'Active'
                  WHEN customer_status = 'I' THEN 'Inactive'
                  ELSE 'Unknown'
                END as customer_status_desc,
                registration_date,
                last_login_date,
                '$batchDate' as etl_date,
                current_timestamp() as etl_timestamp
              FROM ods.customers
              WHERE etl_date = '$batchDate'
                AND customer_id IS NOT NULL
                AND customer_name IS NOT NULL
            """)

        customerDWD.write
          .mode("overwrite")
          .partitionBy("etl_date")
          .saveAsTable("dwd.dim_customer")

        logger.info(s"Successfully processed customer DWD: ${customerDWD.count()} records")
    }

    def processOrderDWD(batchDate: String): Unit = {
        logger.info(s"Processing order DWD data for $batchDate")

        val orderDWD = spark.sql(s"""
              WITH order_enriched AS (
                SELECT
                  o.order_id,
                  o.customer_id,
                  o.order_date,
                  o.order_amount,
                  o.order_status,
                  o.payment_method,
                  c.customer_name,
                  c.age_group,
                  CASE
                    WHEN o.order_amount < 100 THEN 'Small'
                    WHEN o.order_amount BETWEEN 100 AND 500 THEN 'Medium'
                    WHEN o.order_amount BETWEEN 500 AND 1000 THEN 'Large'
                    ELSE 'XLarge'
                  END as order_size_category,
                  YEAR(o.order_date) as order_year,
                  MONTH(o.order_date) as order_month,
                  DAYOFWEEK(o.order_date) as order_day_of_week
                FROM ods.orders o
                LEFT JOIN dwd.dim_customer c ON o.customer_id = c.customer_id
                WHERE o.etl_date = '$batchDate'
                  AND o.order_id IS NOT NULL
                  AND o.customer_id IS NOT NULL
                  AND o.order_amount > 0
              )
              SELECT *,
                '$batchDate' as etl_date,
                current_timestamp() as etl_timestamp
              FROM order_enriched
            """)

        orderDWD.write
          .mode("overwrite")
          .partitionBy("etl_date", "order_year", "order_month")
          .saveAsTable("dwd.fact_order")

        logger.info(s"Successfully processed order DWD: ${orderDWD.count()} records")
    }
}
