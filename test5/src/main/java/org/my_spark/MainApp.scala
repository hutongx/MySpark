package org.my_spark

import org.apache.spark.sql.SparkSession
import org.my_spark.config.AppConfig
import org.my_spark.jobs.{DwdToDwsJob, OdsToDwdJob}
import org.my_spark.utils.SparkSessionUtil
import org.slf4j.{Logger, LoggerFactory}

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object MainApp {
    private val logger: Logger = LoggerFactory.getLogger(this.getClass)

    def main(args: Array[String]): Unit = {
        // Argument parsing (example: job type and load date)
        // A more robust CLI parser like scopt is recommended for production
        if (args.length < 2) {
            logger.error("Usage: MainApp <jobType: ods-dwd|dwd-dws|all> <loadDate: YYYY-MM-DD>")
            System.exit(1)
        }

        val jobType = args(0).toLowerCase
        val loadDateStr = args(1)

        // Validate loadDate
        val loadDate = try {
            LocalDate.parse(loadDateStr, DateTimeFormatter.ISO_LOCAL_DATE).toString
        } catch {
            case e: Exception =>
                logger.error(s"Invalid loadDate format: $loadDateStr. Please use YYYY-MM-DD.", e)
                System.exit(1)
                // This return is for compiler, exit will happen first
                AppConfig.defaultLoadDate
        }

        logger.info(s"Initializing Spark application for jobType: $jobType, loadDate: $loadDate")
        val spark: SparkSession = SparkSessionUtil.getSparkSession(s"${AppConfig.sparkAppName}-$jobType-$loadDate")

        try {
            jobType match {
                case "ods-dwd" =>
                    OdsToDwdJob.run(spark, loadDate)
                case "dwd-dws" =>
                    DwdToDwsJob.run(spark, loadDate)
                case "all" =>
                    logger.info("Running all jobs: ODS to DWD, then DWD to DWS")
                    OdsToDwdJob.run(spark, loadDate)
                    DwdToDwsJob.run(spark, loadDate)
                case _ =>
                    logger.error(s"Unknown jobType: $jobType. Supported types: ods-dwd, dwd-dws, all")
                    System.exit(1)
            }
            logger.info(s"Job(s) '$jobType' completed successfully for loadDate $loadDate.")
        } catch {
            case e: Exception =>
                logger.error(s"Application failed for jobType: $jobType, loadDate: $loadDate. Error: ${e.getMessage}", e)
                System.exit(1) // Ensure YARN marks the application as FAILED
        } finally {
            SparkSessionUtil.stopSparkSession(spark)
            logger.info("Spark session stopped.")
        }
    }
}
