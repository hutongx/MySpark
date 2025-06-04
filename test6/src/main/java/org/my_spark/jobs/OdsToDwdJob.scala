package org.my_spark.jobs

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.my_spark.config.{AppConfig, CliArgs}
import org.my_spark.utils.FileUtil
import org.slf4j.{Logger, LoggerFactory}

object OdsToDwdJob {
    private val logger: Logger = LoggerFactory.getLogger(this.getClass)

    def run(spark: SparkSession, cliArgs: CliArgs): Unit = {
        logger.info(s"Starting ODS to DWD job for loadDate: ${cliArgs.loadDate}, sources: ${cliArgs.sources.mkString(",")}")

        try {
            // Determine which sources to process
            val sourcesToProcess = if (cliArgs.sources.isEmpty || cliArgs.sources.contains("all")) {
                // In a real scenario, you might get this list from config or a discovery mechanism
                List("users", "orders")
            } else {
                cliArgs.sources
            }

            if (sourcesToProcess.contains("users")) {
                processUsers(spark, cliArgs.loadDate.toString)
            }
            if (sourcesToProcess.contains("orders")) {
                processOrders(spark, cliArgs.loadDate.toString)
            }
            // Add more source processing calls here

            logger.info(s"ODS to DWD job completed successfully for loadDate: ${cliArgs.loadDate}")
        } catch {
            case e: Exception =>
                logger.error(s"Error in ODS to DWD job for loadDate ${cliArgs.loadDate}: ${e.getMessage}", e)
                throw e // Re-throw to fail the application and be caught by MainApp
        }
    }

    private def processUsers(spark: SparkSession, loadDate: String): Unit = {
        val sourceName = "users"
        logger.info(s"Processing $sourceName from ODS to DWD for loadDate: $loadDate")

        // 1. Read ODS data
        // Adjust read options based on your ODS data format and partitioning
        // Example: ODS data might be partitioned by event_date or arrival_date.
        // If ODS is Hive table: spark.table(s"ods_db.${sourceName}_raw").where(s"dt='${loadDate}'")
        val odsDataPath = AppConfig.PathConfig.ods(sourceName) // e.g., "hdfs:///dw/ods/users/event_date=${loadDate}"
        logger.info(s"Reading ODS $sourceName data from: $odsDataPath")
        val odsUsersDF: DataFrame = spark.read
          .format("parquet") // Or AppConfig.EtlConfig.inputFormat if ODS format varies
          // .option("header", "true") // for CSV
          // .schema(User.odsSchema) // Define schema for robustness if reading CSV/JSON
          .load(odsDataPath) // If data is partitioned, Spark can infer it from path.

        if (odsUsersDF.isEmpty) {
            logger.warn(s"ODS data for $sourceName at $odsDataPath is empty. Skipping processing.")
            return
        }
        odsUsersDF.createOrReplaceTempView(s"ods_${sourceName}_raw_view")
        logger.info(s"Loaded ODS $sourceName data. Count: ${odsUsersDF.count()}. Schema:")
        odsUsersDF.printSchema()

        // 2. Load and prepare the SQL transformation query
        val sqlFilePath = s"${AppConfig.PathConfig.sqlScriptsBase}/ods_to_dwd/transform_${sourceName}.sql"
        val rawSqlQuery = FileUtil.readResourceFile(sqlFilePath)
        if (rawSqlQuery.isEmpty) {
            logger.error(s"SQL script not found or empty: $sqlFilePath")
            throw new RuntimeException(s"SQL script not found or empty: $sqlFilePath")
        }
        val populatedSQL = rawSqlQuery.replace("${load_date}", loadDate)
        logger.debug(s"Executing DWD transformation SQL for $sourceName:\n$populatedSQL")

        // 3. Apply cleaning logic using SparkSQL
        val dwdCleanedUsersDF: DataFrame = spark.sql(populatedSQL)
        logger.info(s"Transformed DWD cleaned $sourceName. Count: ${dwdCleanedUsersDF.count()}. Schema:")
        dwdCleanedUsersDF.printSchema()
        if (logger.isDebugEnabled) dwdCleanedUsersDF.show(5, truncate = false)

        // 4. Write cleaned data to DWD layer
        // DWD layer is often partitioned by the etl_batch_date or a business date.
        val dwdOutputPath = AppConfig.PathConfig.dwd(s"cleaned_$sourceName")
        val dwdOutputFormat = AppConfig.EtlConfig.outputFormat
        val dwdSaveMode = SaveMode.valueOf(AppConfig.EtlConfig.dwdOutputMode) // Overwrite or Append

        logger.info(s"Writing DWD cleaned $sourceName to: $dwdOutputPath, Format: $dwdOutputFormat, Mode: $dwdSaveMode")
        dwdCleanedUsersDF.write
          .mode(dwdSaveMode)
          .format(dwdOutputFormat)
          .partitionBy("etl_batch_date") // Crucial for DWD data management and incremental loads
          // .option("path", dwdOutputPath) // For non-Hive tables if not using .save() directly
          // .saveAsTable(s"dwd_db.dim_users") // If writing to a Hive managed table
          .save(dwdOutputPath) // For file-based storage (Parquet, Delta)

        logger.info(s"Successfully wrote cleaned $sourceName data to DWD.")
    }

    private def processOrders(spark: SparkSession, loadDate: String): Unit = {
        val sourceName = "orders"
        logger.info(s"Processing $sourceName from ODS to DWD for loadDate: $loadDate")

        val odsDataPath = AppConfig.PathConfig.ods(sourceName)
        logger.info(s"Reading ODS $sourceName data from: $odsDataPath")
        val odsOrdersDF: DataFrame = spark.read.format("parquet").load(odsDataPath)

        if (odsOrdersDF.isEmpty) {
            logger.warn(s"ODS data for $sourceName at $odsDataPath is empty. Skipping processing.")
            return
        }
        odsOrdersDF.createOrReplaceTempView(s"ods_${sourceName}_raw_view")
        logger.info(s"Loaded ODS $sourceName data. Count: ${odsOrdersDF.count()}. Schema:")
        odsOrdersDF.printSchema()

        val sqlFilePath = s"${AppConfig.PathConfig.sqlScriptsBase}/ods_to_dwd/transform_${sourceName}.sql"
        val rawSqlQuery = FileUtil.readResourceFile(sqlFilePath)
        if (rawSqlQuery.isEmpty) {
            logger.error(s"SQL script not found or empty: $sqlFilePath")
            throw new RuntimeException(s"SQL script not found or empty: $sqlFilePath")
        }
        val populatedSQL = rawSqlQuery.replace("${load_date}", loadDate)
        logger.debug(s"Executing DWD transformation SQL for $sourceName:\n$populatedSQL")

        val dwdFactOrdersDF: DataFrame = spark.sql(populatedSQL)
        logger.info(s"Transformed DWD fact $sourceName. Count: ${dwdFactOrdersDF.count()}. Schema:")
        dwdFactOrdersDF.printSchema()
        if (logger.isDebugEnabled) dwdFactOrdersDF.show(5, truncate = false)

        val dwdOutputPath = AppConfig.PathConfig.dwd(s"fact_$sourceName")
        val dwdOutputFormat = AppConfig.EtlConfig.outputFormat
        val dwdSaveMode = SaveMode.valueOf(AppConfig.EtlConfig.dwdOutputMode)

        logger.info(s"Writing DWD fact $sourceName to: $dwdOutputPath, Format: $dwdOutputFormat, Mode: $dwdSaveMode")
        dwdFactOrdersDF.write
          .mode(dwdSaveMode)
          .format(dwdOutputFormat)
          .partitionBy("etl_batch_date")
          .save(dwdOutputPath)

        logger.info(s"Successfully wrote fact $sourceName data to DWD.")
    }
}
