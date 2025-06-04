package org.my_spark

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.logging.log4j.{LogManager, Logger}

/**
 * Main Data Pipeline for ODS -> DWD -> DWS transformation
 * Handles Hive connectivity, data processing, and statistical calculations
 */
object DataPipelineMain {

    private val logger: Logger = LogManager.getLogger(getClass)
    private val config: Config = ConfigFactory.load()

    def main(args: Array[String]): Unit = {
        val pipeline = new DataPipeline()

        try {
            logger.info("Starting data pipeline execution...")

            // Parse command line arguments
            val processDate = if (args.length > 0) args(0) else getCurrentDate()

            // Execute pipeline
            pipeline.execute(processDate)

            logger.info("Data pipeline completed successfully")

        } catch {
            case ex: Exception =>
                logger.error(s"Pipeline execution failed: ${ex.getMessage}", ex)
                System.exit(1)
        } finally {
            pipeline.cleanup()
        }
    }

    private def getCurrentDate(): String = {
        java.time.LocalDate.now().toString
    }
}
