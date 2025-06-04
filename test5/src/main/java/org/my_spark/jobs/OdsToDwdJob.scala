package org.my_spark.jobs

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.my_spark.config.AppConfig
import org.slf4j.{Logger, LoggerFactory}

import scala.io.Source

object OdsToDwdJob {
    private val logger: Logger = LoggerFactory.getLogger(this.getClass)

    def run(spark: SparkSession, loadDate: String): Unit = {
        logger.info(s"Starting ODS to DWD job for load date: $loadDate")

        try {
            // Example: Process Users Data
            processUsers(spark, loadDate)

            // Example: Process Orders Data (similar structure)
            // processOrders(spark, loadDate)

            logger.info(s"ODS to DWD job completed successfully for load date: $loadDate")
        } catch {
            case e: Exception =>
                logger.error(s"Error in ODS to DWD job for load date $loadDate: ${e.getMessage}", e)
                throw e // Re-throw to fail the application
        }
    }

    private def processUsers(spark: SparkSession, loadDate: String): Unit = {
        logger.info("Processing user data from ODS to DWD")

        // 1. Read ODS data (e.g., Parquet, CSV, JDBC, Hive table)
        // Assuming ODS data is partitioned by a date column, e.g., 'event_date'
        // For simplicity, reading a non-partitioned source here.
        // Adjust path and options based on your ODS data source and partitioning scheme.
        val odsUsersDF: DataFrame = spark.read
          .format("parquet") // or "csv", "orc", "json", "jdbc", etc.
          // .option("header", "true") // for CSV
          // .option("inferSchema", "true") // for CSV, can be slow for large files
          .load(AppConfig.getOdsPath("users")) // e.g., "hdfs:///ods/users/dt=${loadDate}" for partitioned data
        // If your ODS table is Hive managed:
        // .table("ods_db.ods_users_raw")
        // .where(s"dt = '$loadDate'")


        odsUsersDF.createOrReplaceTempView("ods_users_raw")
        logger.info(s"Loaded ODS users data. Schema: ${odsUsersDF.schema.treeString}")

        // 2. Load and prepare the SQL transformation query
        val sqlQueryPath = s"${AppConfig.getSqlQueriesPath("ods_to_dwd")}/transform_user_data.sql"
        val sqlQueryTemplate = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream(sqlQueryPath)).mkString
        val populatedSQL = sqlQueryTemplate.replace("${load_date}", loadDate)
        logger.info(s"Executing DWD transformation SQL: \n$populatedSQL")


        // 3. Apply cleaning logic using SparkSQL
        val dwdCleanedUsersDF: DataFrame = spark.sql(populatedSQL)
        logger.info(s"Transformed DWD cleaned users. Schema: ${dwdCleanedUsersDF.schema.treeString}")
        dwdCleanedUsersDF.show(5, truncate = false) // For debugging

        // 4. Write cleaned data to DWD layer
        // Consider partitioning by etl_load_date or another relevant date column for DWD
        val dwdOutputPath = AppConfig.getDwdPath("cleaned_users")
        logger.info(s"Writing DWD cleaned users to: $dwdOutputPath (partitioned by etl_load_date)")

        dwdCleanedUsersDF.write
          .mode(SaveMode.valueOf(AppConfig.outputMode)) // Overwrite or Append
          .format(AppConfig.outputFormat) // parquet, delta, orc
          .partitionBy("etl_load_date") // Important for managing DWD data lifecycle
          // .option("path", dwdOutputPath) // For non-Hive tables
          // .saveAsTable("dwd_db.dim_users") // If writing to a Hive table
          .save(dwdOutputPath) // For file-based storage

        logger.info("Successfully wrote cleaned user data to DWD.")
    }

    // Implement processOrders similarly...
    // private def processOrders(spark: SparkSession, loadDate: String): Unit = { ... }
}
