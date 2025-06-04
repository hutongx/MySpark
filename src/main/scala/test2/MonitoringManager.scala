package test2

import org.apache.spark.sql._

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

// ===== MONITORING AND ALERTING =====
class MonitoringManager(spark: SparkSession) {

    def monitorJobProgress(jobName: String): Unit = {
        val sc = spark.sparkContext
        val statusTracker = sc.statusTracker

        println(s"Monitoring job: $jobName")
        println(s"Active Jobs: ${statusTracker.getActiveJobIds().length}")
        println(s"Active Stages: ${statusTracker.getActiveStageIds().length}")

        // Log resource usage
        val executorInfos = statusTracker.getExecutorInfos
        executorInfos.foreach { info =>
            println(s"Executor ${info.executorId}: ${info.totalCores} cores, ${info.maxMemory} memory")
        }
    }

    def generateExecutionReport(jobName: String, startTime: Long, endTime: Long): Unit = {
        val duration = (endTime - startTime) / 1000.0
        val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))

        println(s"""
                   |=============== EXECUTION REPORT ===============
                   |Job Name: $jobName
                   |Start Time: ${new java.util.Date(startTime)}
                   |End Time: ${new java.util.Date(endTime)}
                   |Duration: $duration seconds
                   |Status: SUCCESS
                   |Report Generated: $timestamp
                   |===============================================
    """.stripMargin)
    }
}
