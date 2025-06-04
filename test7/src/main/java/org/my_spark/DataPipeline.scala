package org.my_spark

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.logging.log4j.{LogManager, Logger}

/**
 * Core Data Pipeline Class
 */
class DataPipeline {

    private val logger: Logger = LogManager.getLogger(getClass)
    private val config: Config = ConfigFactory.load()

    // Spark Session with Hive support
    private lazy val spark: SparkSession = createSparkSession()

    // Database names from configuration
    private val odsDb = config.getString("database.ods")
    private val dwdDb = config.getString("database.dwd")
    private val dwsDb = config.getString("database.dws")

    /**
     * Create Spark Session with Hive integration
     */
    private def createSparkSession(): SparkSession = {
        logger.info("Initializing Spark Session with Hive support...")

        val session = SparkSession.builder()
          .appName(config.getString("spark.app-name"))
          .config("spark.sql.adaptive.enabled",
              config.getBoolean("spark.sql.adaptive.enabled"))
          .config("spark.sql.adaptive.coalescePartitions.enabled",
              config.getBoolean("spark.sql.adaptive.coalescePartitions.enabled"))
          .config("spark.sql.adaptive.skewJoin.enabled",
              config.getBoolean("spark.sql.adaptive.skewJoin.enabled"))
          .config("hive.metastore.uris",
              config.getString("spark.hive.metastore.uris"))
          .config("hive.exec.dynamic.partition",
              config.getBoolean("spark.hive.exec.dynamic.partition"))
          .config("hive.exec.dynamic.partition.mode",
              config.getString("spark.hive.exec.dynamic.partition.mode"))
          .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .enableHiveSupport()
          .getOrCreate()

        // Set log level
        session.sparkContext.setLogLevel("WARN")

        logger.info("Spark Session initialized successfully")
        session
    }

    /**
     * Main execution method
     */
    def execute(processDate: String): Unit = {
        logger.info(s"Starting pipeline execution for date: $processDate")

        // Step 1: Create databases if not exist
        createDatabases()

        // Step 2: Extract data from ODS
        val odsData = extractFromODS(processDate)

        // Step 3: Transform to DWD layer
        val dwdData = transformToDWD(odsData, processDate)

        // Step 4: Load to DWD tables
        loadToDWD(dwdData, processDate)

        // Step 5: Calculate statistics for DWS layer
        val dwsData = calculateDWSStatistics(processDate)

        // Step 6: Load to DWS tables
        loadToDWS(dwsData, processDate)

        logger.info("Pipeline execution completed successfully")
    }

    /**
     * Create databases if they don't exist
     */
    private def createDatabases(): Unit = {
        logger.info("Creating databases...")

        spark.sql(s"CREATE DATABASE IF NOT EXISTS $odsDb")
        spark.sql(s"CREATE DATABASE IF NOT EXISTS $dwdDb")
        spark.sql(s"CREATE DATABASE IF NOT EXISTS $dwsDb")

        logger.info("Databases created/verified successfully")
    }

    /**
     * Extract data from ODS layer
     */
    private def extractFromODS(processDate: String): Map[String, DataFrame] = {
        logger.info(s"Extracting data from ODS for date: $processDate")

        val extractedData = Map(
            "customer" -> extractCustomerData(processDate),
            "order" -> extractOrderData(processDate),
            "product" -> extractProductData(processDate)
        )

        // Data quality checks
        extractedData.foreach { case (tableName, df) =>
            val count = df.count()
            logger.info(s"Extracted $count records from $tableName")

            if (count == 0) {
                logger.warn(s"No data found in $tableName for date $processDate")
            }
        }

        extractedData
    }

    /**
     * Extract customer data from ODS
     */
    private def extractCustomerData(processDate: String): DataFrame = {
        spark.sql(s"""
      SELECT
        customer_id,
        customer_name,
        email,
        phone,
        address,
        city,
        state,
        country,
        registration_date,
        last_update_time,
        is_active
      FROM $odsDb.customer_info
      WHERE date_format(last_update_time, 'yyyy-MM-dd') = '$processDate'
      AND is_active = 1
    """)
    }

    /**
     * Extract order data from ODS
     */
    private def extractOrderData(processDate: String): DataFrame = {
        spark.sql(s"""
      SELECT
        order_id,
        customer_id,
        product_id,
        order_date,
        quantity,
        unit_price,
        total_amount,
        order_status,
        payment_method,
        created_time
      FROM $odsDb.order_info
      WHERE date_format(order_date, 'yyyy-MM-dd') = '$processDate'
      AND order_status != 'CANCELLED'
    """)
    }

    /**
     * Extract product data from ODS
     */
    private def extractProductData(processDate: String): DataFrame = {
        spark.sql(s"""
      SELECT
        product_id,
        product_name,
        category,
        subcategory,
        brand,
        price,
        cost,
        supplier_id,
        last_update_time
      FROM $odsDb.product_info
      WHERE date_format(last_update_time, 'yyyy-MM-dd') = '$processDate'
    """)
    }

    /**
     * Transform data to DWD layer with business logic
     */
    private def transformToDWD(odsData: Map[String, DataFrame], processDate: String): Map[String, DataFrame] = {
        logger.info("Transforming data to DWD layer...")

        val customerDf = odsData("customer")
        val orderDf = odsData("order")
        val productDf = odsData("product")

        // Transform customer dimension
        val dwdCustomer = customerDf
          .withColumn("customer_age_group",
              when(datediff(current_date(), col("registration_date")) < 365, "New")
                .when(datediff(current_date(), col("registration_date")) < 1095, "Regular")
                .otherwise("Loyal"))
          .withColumn("email_domain",
              regexp_extract(col("email"), "@(.+)", 1))
          .withColumn("process_date", lit(processDate))
          .withColumn("etl_time", current_timestamp())

        // Transform order fact with enrichment
        val dwdOrder = orderDf
          .join(customerDf.select("customer_id", "customer_name", "city", "state"),
              Seq("customer_id"), "left")
          .join(productDf.select("product_id", "product_name", "category", "brand"),
              Seq("product_id"), "left")
          .withColumn("profit", col("total_amount") - (col("quantity") * col("cost")))
          .withColumn("order_year", year(col("order_date")))
          .withColumn("order_month", month(col("order_date")))
          .withColumn("order_quarter", quarter(col("order_date")))
          .withColumn("process_date", lit(processDate))
          .withColumn("etl_time", current_timestamp())

        // Transform product dimension
        val dwdProduct = productDf
          .withColumn("profit_margin",
              round((col("price") - col("cost")) / col("price") * 100, 2))
          .withColumn("price_category",
              when(col("price") < 50, "Low")
                .when(col("price") < 200, "Medium")
                .otherwise("High"))
          .withColumn("process_date", lit(processDate))
          .withColumn("etl_time", current_timestamp())

        Map(
            "customer" -> dwdCustomer,
            "order" -> dwdOrder,
            "product" -> dwdProduct
        )
    }

    /**
     * Load data to DWD tables
     */
    private def loadToDWD(dwdData: Map[String, DataFrame], processDate: String): Unit = {
        logger.info("Loading data to DWD layer...")

        // Load customer dimension
        dwdData("customer")
          .write
          .mode("overwrite")
          .option("path", s"/user/hive/warehouse/$dwdDb.db/dim_customer/process_date=$processDate")
          .saveAsTable(s"$dwdDb.dim_customer")

        // Load order fact
        dwdData("order")
          .write
          .mode("overwrite")
          .partitionBy("order_year", "order_month")
          .option("path", s"/user/hive/warehouse/$dwdDb.db/fact_order")
          .saveAsTable(s"$dwdDb.fact_order")

        // Load product dimension
        dwdData("product")
          .write
          .mode("overwrite")
          .option("path", s"/user/hive/warehouse/$dwdDb.db/dim_product/process_date=$processDate")
          .saveAsTable(s"$dwdDb.dim_product")

        logger.info("DWD data loaded successfully")
    }

    /**
     * Calculate statistical aggregations for DWS layer
     */
    private def calculateDWSStatistics(processDate: String): Map[String, DataFrame] = {
        logger.info("Calculating DWS statistics...")

        val dailySales = calculateDailySalesStats(processDate)
        val customerStats = calculateCustomerStats(processDate)
        val productStats = calculateProductStats(processDate)
        val monthlyTrends = calculateMonthlyTrends(processDate)

        Map(
            "daily_sales" -> dailySales,
            "customer_stats" -> customerStats,
            "product_stats" -> productStats,
            "monthly_trends" -> monthlyTrends
        )
    }

    /**
     * Calculate daily sales statistics
     */
    private def calculateDailySalesStats(processDate: String): DataFrame = {
        spark.sql(s"""
      SELECT
        '$processDate' as report_date,
        COUNT(DISTINCT order_id) as total_orders,
        COUNT(DISTINCT customer_id) as unique_customers,
        SUM(total_amount) as total_revenue,
        AVG(total_amount) as avg_order_value,
        SUM(quantity) as total_quantity,
        SUM(profit) as total_profit,
        AVG(profit) as avg_profit_per_order,
        MAX(total_amount) as max_order_value,
        MIN(total_amount) as min_order_value,
        STDDEV(total_amount) as order_value_std,
        current_timestamp() as etl_time
      FROM $dwdDb.fact_order
      WHERE date_format(order_date, 'yyyy-MM-dd') = '$processDate'
    """)
    }

    /**
     * Calculate customer statistics
     */
    private def calculateCustomerStats(processDate: String): DataFrame = {
        spark.sql(s"""
      SELECT
        '$processDate' as report_date,
        customer_age_group,
        state,
        COUNT(DISTINCT customer_id) as customer_count,
        COUNT(order_id) as total_orders,
        SUM(total_amount) as total_spending,
        AVG(total_amount) as avg_order_value,
        SUM(profit) as total_profit_contribution,
        current_timestamp() as etl_time
      FROM $dwdDb.fact_order fo
      JOIN $dwdDb.dim_customer dc ON fo.customer_id = dc.customer_id
      WHERE date_format(fo.order_date, 'yyyy-MM-dd') = '$processDate'
        AND dc.process_date = '$processDate'
      GROUP BY customer_age_group, state
    """)
    }

    /**
     * Calculate product statistics
     */
    private def calculateProductStats(processDate: String): DataFrame = {
        spark.sql(s"""
      SELECT
        '$processDate' as report_date,
        category,
        brand,
        price_category,
        COUNT(DISTINCT fo.product_id) as products_sold,
        SUM(quantity) as total_quantity_sold,
        SUM(total_amount) as total_revenue,
        AVG(total_amount) as avg_revenue_per_product,
        SUM(profit) as total_profit,
        AVG(profit_margin) as avg_profit_margin,
        current_timestamp() as etl_time
      FROM $dwdDb.fact_order fo
      JOIN $dwdDb.dim_product dp ON fo.product_id = dp.product_id
      WHERE date_format(fo.order_date, 'yyyy-MM-dd') = '$processDate'
        AND dp.process_date = '$processDate'
      GROUP BY category, brand, price_category
    """)
    }

    /**
     * Calculate monthly trends
     */
    private def calculateMonthlyTrends(processDate: String): DataFrame = {
        spark.sql(s"""
      SELECT
        order_year,
        order_month,
        order_quarter,
        COUNT(DISTINCT order_id) as monthly_orders,
        COUNT(DISTINCT customer_id) as monthly_customers,
        SUM(total_amount) as monthly_revenue,
        SUM(profit) as monthly_profit,
        AVG(total_amount) as avg_monthly_order_value,
        (SUM(profit) / SUM(total_amount)) * 100 as profit_margin_pct,
        current_timestamp() as etl_time
      FROM $dwdDb.fact_order
      WHERE order_year = year('$processDate')
        AND order_month = month('$processDate')
      GROUP BY order_year, order_month, order_quarter
    """)
    }

    /**
     * Load statistics to DWS tables
     */
    private def loadToDWS(dwsData: Map[String, DataFrame], processDate: String): Unit = {
        logger.info("Loading statistics to DWS layer...")

        // Load daily sales statistics
        dwsData("daily_sales")
          .write
          .mode("overwrite")
          .option("path", s"/user/hive/warehouse/$dwsDb.db/daily_sales_stats/report_date=$processDate")
          .saveAsTable(s"$dwsDb.daily_sales_stats")

        // Load customer statistics
        dwsData("customer_stats")
          .write
          .mode("overwrite")
          .partitionBy("customer_age_group", "state")
          .option("path", s"/user/hive/warehouse/$dwsDb.db/customer_stats")
          .saveAsTable(s"$dwsDb.customer_stats")

        // Load product statistics
        dwsData("product_stats")
          .write
          .mode("overwrite")
          .partitionBy("category", "brand")
          .option("path", s"/user/hive/warehouse/$dwsDb.db/product_stats")
          .saveAsTable(s"$dwsDb.product_stats")

        // Load monthly trends
        dwsData("monthly_trends")
          .write
          .mode("overwrite")
          .partitionBy("order_year", "order_month")
          .option("path", s"/user/hive/warehouse/$dwsDb.db/monthly_trends")
          .saveAsTable(s"$dwsDb.monthly_trends")

        logger.info("DWS statistics loaded successfully")
    }

    /**
     * Cleanup resources
     */
    def cleanup(): Unit = {
        if (spark != null) {
            logger.info("Cleaning up Spark session...")
            spark.stop()
        }
    }
}
