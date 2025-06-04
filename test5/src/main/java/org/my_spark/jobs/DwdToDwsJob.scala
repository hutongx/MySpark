package org.my_spark.jobs

import org.my_spark.config.AppConfig
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import scala.io.Source

object DwdToDwsJob {
    private val logger: Logger = LoggerFactory.getLogger(this.getClass)

    def run(spark: SparkSession, loadDate: String): Unit = {
        logger.info(s"Starting DWD to DWS job for load date: $loadDate")

        try {
            // Example: Create Daily Sales Aggregate
            createDailySalesAggregate(spark, loadDate)

            // Example: Create User Summary Table (if you have user dimension and facts)
            // createUserSummary(spark, loadDate)

            logger.info(s"DWD to DWS job completed successfully for load date: $loadDate")
        } catch {
            case e: Exception =>
                logger.error(s"Error in DWD to DWS job for load date $loadDate: ${e.getMessage}", e)
                throw e
        }
    }

    private def createDailySalesAggregate(spark: SparkSession, loadDate: String): Unit = {
        logger.info("Creating DWS daily sales aggregate")

        // 1. Read DWD data
        // Assume DWD fact_orders is partitioned by etl_load_date
        val dwdFactOrdersDF: DataFrame = spark.read
          .format(AppConfig.outputFormat)
          .option("path", s"${AppConfig.getDwdPath("fact_orders")}/etl_load_date=$loadDate") // Read specific partition
          // If DWD is Hive table:
          // .table("dwd_db.fact_orders")
          // .where(s"etl_load_date = '$loadDate'")
          .load() // path provided in option

        // Assume DWD dim_products is also partitioned or you load the relevant version
        // For simplicity, let's assume it's also partitioned by etl_load_date (or it's a full load/SCD Type 1)
        val dwdDimProductsDF: DataFrame = spark.read
          .format(AppConfig.outputFormat)
          .option("path", s"${AppConfig.getDwdPath("dim_products")}/etl_load_date=$loadDate") // Adjust as per your dim strategy
          .load()

        if (dwdFactOrdersDF.isEmpty || dwdDimProductsDF.isEmpty) {
            logger.warn(s"One or both DWD input DataFrames are empty for loadDate: $loadDate. Skipping DWS aggregation for daily sales.")
            return
        }

        dwdFactOrdersDF.createOrReplaceTempView("dwd_fact_orders")
        dwdDimProductsDF.createOrReplaceTempView("dwd_dim_products")

        logger.info(s"Loaded DWD fact_orders. Schema: ${dwdFactOrdersDF.schema.treeString}")
        logger.info(s"Loaded DWD dim_products. Schema: ${dwdDimProductsDF.schema.treeString}")


        // 2. Load and prepare the SQL transformation query
        val sqlQueryPath = s"${AppConfig.getSqlQueriesPath("dwd_to_dws")}/aggregate_sales_daily.sql"
        val sqlQueryTemplate = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream(sqlQueryPath)).mkString
        val populatedSQL = sqlQueryTemplate.replace("${load_date}", loadDate)
        logger.info(s"Executing DWS aggregation SQL: \n$populatedSQL")

        // 3. Apply aggregation logic using SparkSQL
        val dwsDailySalesAggDF: DataFrame = spark.sql(populatedSQL)
        logger.info(s"Aggregated DWS daily sales. Schema: ${dwsDailySalesAggDF.schema.treeString}")
        dwsDailySalesAggDF.show(5, truncate = false)

        // 4. Write aggregated data to DWS layer
        val dwsOutputPath = AppConfig.getDwsPath("daily_sales_agg")
        logger.info(s"Writing DWS daily sales aggregate to: $dwsOutputPath (partitioned by etl_load_date)")

        dwsDailySalesAggDF.write
          .mode(SaveMode.valueOf(AppConfig.outputMode))
          .format(AppConfig.outputFormat)
          .partitionBy("etl_load_date") // DWS tables are often partitioned by processing date or event date
          // .option("path", dwsOutputPath)
          // .saveAsTable("dws_db.daily_sales_summary")
          .save(dwsOutputPath)

        logger.info("Successfully wrote daily sales aggregate to DWS.")
    }

    // Implement createUserSummary similarly...
    // private def createUserSummary(spark: SparkSession, loadDate: String): Unit = { ... }
}
