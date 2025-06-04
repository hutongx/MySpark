package org.my_spark

import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession
import org.my_spark.model.JobMetrics

/**
 * Performance monitoring utilities
 */
object PerformanceMonitor {

    private val logger: Logger = LogManager.getLogger(getClass)

    /**
     * Monitor and log DataFrame operations
     */
    def monitorOperation[T](operationName: String, tableName: String)(operation: => T): T = {
        val startTime = System.currentTimeMillis()
        logger.info(s"Starting operation: $operationName for table: $tableName")

        try {
            val result = operation
            val endTime = System.currentTimeMillis()
            val duration = endTime - startTime

            logger.info(s"Operation completed: $operationName for table: $tableName in ${duration}ms")
            result
        } catch {
            case ex: Exception =>
                val endTime = System.currentTimeMillis()
                val duration = endTime - startTime
                logger.error(s"Operation failed: $operationName for table: $tableName after ${duration}ms", ex)
                throw ex
        }
    }

    /**
     * Get Spark job metrics
     */
    def getJobMetrics(spark: SparkSession): JobMetrics = {
        val sc = spark.sparkContext
        val statusTracker = sc.statusTracker

        JobMetrics(
            applicationId = sc.applicationId,
            applicationName = sc.appName,
            executorInfos = statusTracker.getExecutorInfos.length,
            activeStages = statusTracker.getActiveStageIds.length,
            activeJobs = statusTracker.getActiveJobIds.length
        )
    }
}
