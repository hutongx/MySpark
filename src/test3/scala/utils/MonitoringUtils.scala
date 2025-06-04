package utils

import job.PipelineResult
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object MonitoringUtils {
    private val logger = LoggerFactory.getLogger(this.getClass)

    def sendPipelineMetrics(result: PipelineResult, batchDate: String): Unit = {
        // In production, integrate with monitoring systems like Prometheus/Grafana
        logger.info("=== Pipeline Execution Summary ===")
        logger.info(s"Batch Date: $batchDate")
        logger.info(s"Overall Success: ${result.success}")
        logger.info(s"ODS Layer: ${if (result.odsSuccess) "SUCCESS" else "FAILED"}")
        logger.info(s"DWD Layer: ${if (result.dwdSuccess) "SUCCESS" else "FAILED"}")
        logger.info(s"DWS Layer: ${if (result.dwsSuccess) "SUCCESS" else "FAILED"}")
        logger.info(s"Execution Time: ${result.executionTimeMs}ms")

        if (result.errors.nonEmpty) {
            logger.error(s"Errors encountered: ${result.errors.mkString("; ")}")
        }

        // Save metrics to monitoring table
        savePipelineMetrics(result, batchDate)
    }

    private def savePipelineMetrics(result: PipelineResult, batchDate: String): Unit = {
        // Implementation to save metrics to database/monitoring system
        logger.info("Pipeline metrics saved to monitoring system")
    }

    def checkDataFreshness(spark: SparkSession, tableName: String,
                           expectedDate: String): Boolean = {
        try {
            val latestDate = spark.sql(s"SELECT MAX(etl_date) FROM $tableName").collect()(0).getString(0)
            val isFresh = latestDate == expectedDate

            if (!isFresh) {
                logger.warn(s"Data freshness check failed for $tableName. Expected: $expectedDate, Found: $latestDate")
            }

            isFresh
        } catch {
            case e: Exception =>
                logger.error(s"Error checking data freshness for $tableName", e)
                false
        }
    }
}
