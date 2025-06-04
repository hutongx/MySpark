package layers

import org.apache.spark
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory
import utils.DataQualityUtils

class ODSProcessor(spark: SparkSession)  {
    import spark.implicits._
    private val logger = LoggerFactory.getLogger(this.getClass)

    def processCustomerData(batchDate: String): Unit = {
        logger.info(s"Processing customer ODS data for $batchDate")

        try {
            // Read from source system (example: MySQL via JDBC)
            val customerDF = spark.read
              .format("jdbc")
              .option("url", "jdbc:mysql://localhost:3306/source_db")
              .option("dbtable", s"(SELECT * FROM customers WHERE update_date = '$batchDate') as customer_data")
              .option("user", "username")
              .option("password", "password")
              .load()

            // Add ETL metadata
            val processedDF = customerDF
              .withColumn("etl_date", lit(batchDate))
              .withColumn("etl_timestamp", current_timestamp())
              .withColumn("data_source", lit("mysql_customers"))

            // Data quality checks
            val validatedDF = DataQualityUtils.validateCustomerData(processedDF)

            // Write to Hive ODS table
            validatedDF.write
              .mode("overwrite")
              .option("path", s"/user/hive/warehouse/ods.db/customers/dt=$batchDate")
              .saveAsTable("ods.customers")

            logger.info(s"Successfully processed ${validatedDF.count()} customer records")

        } catch {
            case e: Exception =>
                logger.error(s"Error processing customer ODS data for $batchDate", e)
                throw e
        }
    }

    def processOrderData(batchDate: String): Unit = {
        logger.info(s"Processing order ODS data for $batchDate")

        try {
            val orderDF = spark.read
              .format("jdbc")
              .option("url", "jdbc:mysql://localhost:3306/source_db")
              .option("dbtable", s"(SELECT * FROM orders WHERE order_date = '$batchDate') as order_data")
              .option("user", "username")
              .option("password", "password")
              .load()

            val processedDF = orderDF
              .withColumn("etl_date", lit(batchDate))
              .withColumn("etl_timestamp", current_timestamp())
              .withColumn("data_source", lit("mysql_orders"))

            processedDF.write
              .mode("overwrite")
              .partitionBy("etl_date")
              .saveAsTable("ods.orders")

            logger.info(s"Successfully processed ${processedDF.count()} order records")

        } catch {
            case e: Exception =>
                logger.error(s"Error processing order ODS data for $batchDate", e)
                throw e
        }
    }
}
