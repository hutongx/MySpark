package job

import config.SparkConfig
import layers.{DWDProcessor, DWSProcessor, ODSProcessor}
import org.slf4j.LoggerFactory

object SparkHiveDWHApp {
    private val logger = LoggerFactory.getLogger(this.getClass)

    def main(args: Array[String]): Unit = {
        val spark = SparkConfig.createSparkSession("SparkHiveDWH")

        try {
            logger.info("Starting Spark Hive DWH processing...")

            // Initialize processors
            val odsProcessor = new ODSProcessor(spark)
            val dwdProcessor = new DWDProcessor(spark)
            val dwsProcessor = new DWSProcessor(spark)

            // Process data through layers
            val batchDate = if (args.length > 0) args(0) else getCurrentDate()

            // ODS Layer - Raw data ingestion
            logger.info(s"Processing ODS layer for date: $batchDate")
            odsProcessor.processCustomerData(batchDate)
            odsProcessor.processOrderData(batchDate)

            // DWD Layer - Data cleaning and transformation
            logger.info(s"Processing DWD layer for date: $batchDate")
            dwdProcessor.processCustomerDWD(batchDate)
            dwdProcessor.processOrderDWD(batchDate)

            // DWS Layer - Statistical calculations
            logger.info(s"Processing DWS layer for date: $batchDate")
            dwsProcessor.generateCustomerSummary(batchDate)
            dwsProcessor.generateOrderStatistics(batchDate)

            logger.info("Spark Hive DWH processing completed successfully!")

        } catch {
            case e: Exception =>
                logger.error("Error in Spark Hive DWH processing", e)
                throw e
        } finally {
            spark.stop()
        }
    }

    private def getCurrentDate(): String = {
        java.time.LocalDate.now().toString
    }
}
