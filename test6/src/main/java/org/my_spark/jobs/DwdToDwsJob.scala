package org.my_spark.jobs

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.my_spark.config.{AppConfig, CliArgs}
import org.my_spark.utils.FileUtil
import org.slf4j.{Logger, LoggerFactory}

object DwdToDwsJob {
    private val logger: Logger = LoggerFactory.getLogger(this.getClass)

    def run(spark: SparkSession, cliArgs: CliArgs): Unit = {
        logger.info(s"Starting DWD to DWS job for loadDate: ${cliArgs.loadDate}, topics: ${cliArgs.sources.mkString(",")}")

        try {
            val topicsToProcess = if (cliArgs.sources.isEmpty || cliArgs.sources.contains("all")) {
                List("daily_sales", "user_summary") // Example DWS topics
            } else {
                cliArgs.sources
            }

            if (topicsToProcess.contains("daily_sales")) {
                createDailySalesAggregate(spark, cliArgs.loadDate.toString)
            }
            if (topicsToProcess.contains("user_summary")) {
                createUserProfileSummary(spark, cliArgs.loadDate.toString)
            }
            // Add more DWS topic creation calls

            logger.info(s"DWD to DWS job completed successfully for loadDate: ${cliArgs.loadDate}")
        } catch {
            case e: Exception =>
                logger.error(s"Error in DWD to DWS job for loadDate ${cliArgs.loadDate}: ${e.getMessage}", e)
                throw e
        }
    }

    private def createDailySalesAggregate(spark: SparkSession, loadDate: String): Unit = {
        val topicName = "daily_sales_agg" // Matches config: paths.dws.product_sales_agg
        val dwsTableName = "product_sales_agg" // As in AppConfig.PathConfig.dws(...)
        logger.info(s"Creating DWS topic '$topicName' for loadDate: $loadDate")

        // 1. Read DWD data required for this DWS table
        // Example: Read DWD fact_orders for the given loadDate partition
        val dwdFactOrdersPath = s"${AppConfig.PathConfig.dwd("fact_orders")}/etl_batch_date=$loadDate"
        logger.info(s"Reading DWD fact_orders from: $dwdFactOrdersPath")
        val dwdFactOrdersDF: DataFrame = spark.read.format(AppConfig.EtlConfig.outputFormat).load(dwdFactOrdersPath)

        if (dwdFactOrdersDF.isEmpty) {
            logger.warn(s"DWD fact_orders data for etl_batch_date=$loadDate is empty. Skipping $topicName aggregation.")
            return
        }
        dwdFactOrdersDF.createOrReplaceTempView("dwd_fact_orders_view")
        logger.info(s"Loaded DWD fact_orders. Count: ${dwdFactOrdersDF.count()}. Schema:")
        dwdFactOrdersDF.printSchema()

        // If you need other DWD tables (e.g., dim_products), load them similarly:
        // val dwdDimProductsPath = s"${AppConfig.PathConfig.dwd("dim_products")}/etl_batch_date=$loadDate"
        // val dwdDimProductsDF = spark.read.format(AppConfig.EtlConfig.outputFormat).load(dwdDimProductsPath)
        // dwdDimProductsDF.createOrReplaceTempView("dwd_dim_products_view")

        // 2. Load and prepare the SQL aggregation query
        val sqlFilePath = s"${AppConfig.PathConfig.sqlScriptsBase}/dwd_to_dws/aggregate_daily_sales.sql"
        val rawSqlQuery = FileUtil.readResourceFile(sqlFilePath)
        if (rawSqlQuery.isEmpty) {
            logger.error(s"SQL script not found or empty: $sqlFilePath")
            throw new RuntimeException(s"SQL script not found or empty: $sqlFilePath")
        }
        val populatedSQL = rawSqlQuery.replace("${load_date}", loadDate)
        logger.debug(s"Executing DWS aggregation SQL for $topicName:\n$populatedSQL")

        // 3. Apply aggregation logic using SparkSQL
        val dwsDailySalesAggDF: DataFrame = spark.sql(populatedSQL)
        logger.info(s"Aggregated DWS $topicName. Count: ${dwsDailySalesAggDF.count()}. Schema:")
        dwsDailySalesAggDF.printSchema()
        if(logger.isDebugEnabled) dwsDailySalesAggDF.show(5, truncate = false)

        // 4. Write aggregated data to DWS layer
        val dwsOutputPath = AppConfig.PathConfig.dws(dwsTableName)
        val dwsOutputFormat = AppConfig.EtlConfig.outputFormat
        val dwsSaveMode = SaveMode.valueOf(AppConfig.EtlConfig.dwsOutputMode)

        logger.info(s"Writing DWS $topicName to: $dwsOutputPath, Format: $dwsOutputFormat, Mode: $dwsSaveMode")
        dwsDailySalesAggDF.write
          .mode(dwsSaveMode)
          .format(dwsOutputFormat)
          .partitionBy("etl_batch_date") // DWS tables also often partitioned
          .save(dwsOutputPath)

        logger.info(s"Successfully wrote DWS $topicName data.")
    }

    private def createUserProfileSummary(spark: SparkSession, loadDate: String): Unit = {
        val topicName = "user_profile_summary"
        val dwsTableName = "user_summary_daily" // As in AppConfig.PathConfig.dws(...)
        logger.info(s"Creating DWS topic '$topicName' for loadDate: $loadDate")

        // Load DWD cleaned_users for the current batch date
        val dwdCleanedUsersPath = s"${AppConfig.PathConfig.dwd("cleaned_users")}/etl_batch_date=$loadDate"
        logger.info(s"Reading DWD cleaned_users from: $dwdCleanedUsersPath")
        val dwdCleanedUsersDF = spark.read.format(AppConfig.EtlConfig.outputFormat).load(dwdCleanedUsersPath)

        // Load DWD fact_orders. For lifetime summaries, you might need to read all historical partitions
        // or use an existing DWD aggregate if available. For this example, we'll read all partitions up to loadDate.
        // This can be inefficient for very large history; consider pre-aggregates or Delta Lake time travel.
        val dwdFactOrdersBasePath = AppConfig.PathConfig.dwd("fact_orders")
        logger.info(s"Reading DWD fact_orders from base path: $dwdFactOrdersBasePath for history up to $loadDate")
        val dwdFactOrdersDF = spark.read.format(AppConfig.EtlConfig.outputFormat).load(dwdFactOrdersBasePath)
        // .where(s"etl_batch_date <= '$loadDate'") // Filter if reading from a non-partitioned historical table

        if (dwdCleanedUsersDF.isEmpty) {
            logger.warn(s"DWD cleaned_users data for etl_batch_date=$loadDate is empty. Skipping $topicName creation.")
            return
        }
        dwdCleanedUsersDF.createOrReplaceTempView("dwd_cleaned_users_view")
        dwdFactOrdersDF.createOrReplaceTempView("dwd_fact_orders_view") // Contains all history or relevant window

        logger.info(s"Loaded DWD cleaned_users (count: ${dwdCleanedUsersDF.count()}) and fact_orders (count: ${dwdFactOrdersDF.count()})")

        val sqlFilePath = s"${AppConfig.PathConfig.sqlScriptsBase}/dwd_to_dws/create_user_profile_summary.sql"
        val rawSqlQuery = FileUtil.readResourceFile(sqlFilePath)
        if (rawSqlQuery.isEmpty) {
            logger.error(s"SQL script not found or empty: $sqlFilePath")
            throw new RuntimeException(s"SQL script not found or empty: $sqlFilePath")
        }
        val populatedSQL = rawSqlQuery.replace("${load_date}", loadDate)
        logger.debug(s"Executing DWS aggregation SQL for $topicName:\n$populatedSQL")

        val dwsUserSummaryDF: DataFrame = spark.sql(populatedSQL)
        logger.info(s"Generated DWS $topicName. Count: ${dwsUserSummaryDF.count()}. Schema:")
        dwsUserSummaryDF.printSchema()
        if(logger.isDebugEnabled) dwsUserSummaryDF.show(5, truncate = false)

        val dwsOutputPath = AppConfig.PathConfig.dws(dwsTableName)
        val dwsOutputFormat = AppConfig.EtlConfig.outputFormat
        // User summaries are often full rebuilds or handled with merge/upsert logic (e.g. with Delta Lake)
        val dwsSaveMode = SaveMode.valueOf(AppConfig.EtlConfig.dwsOutputMode)


        logger.info(s"Writing DWS $topicName to: $dwsOutputPath, Format: $dwsOutputFormat, Mode: $dwsSaveMode")
        dwsUserSummaryDF.write
          .mode(dwsSaveMode)
          .format(dwsOutputFormat)
          .partitionBy("etl_batch_date") // Or perhaps no partitioning if it's a snapshot table
          .save(dwsOutputPath)

        logger.info(s"Successfully wrote DWS $topicName data.")
    }
}
