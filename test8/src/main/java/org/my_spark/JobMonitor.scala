package org.my_spark

import org.my_spark.DataWarehouseJob.JobParams
import org.slf4j.{Logger, LoggerFactory}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.mutable
import scala.language.postfixOps

/**
 * 任务监控器
 * 负责监控任务执行状态，记录执行日志，发送告警通知
 */
class JobMonitor(jobId: String) {
    private val logger: Logger = LoggerFactory.getLogger(getClass)
    private val startTime = System.currentTimeMillis()
    private var currentStatus = "INIT"
    private var currentProgress = 0.0
    private var currentMessage = ""
    private val metrics = mutable.Map[String, Any]()

    /**
     * 启动任务监控
     */
    def startJob(params: JobParams): Unit = {
        currentStatus = "RUNNING"
        val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
        logger.info(s"任务监控启动: $jobId")
        logger.info(s"开始时间: $timestamp")
        logger.info(s"任务参数: $params")

        // 记录任务启动信息
        metrics += (
          "job_id" -> jobId,
          "start_time" -> startTime,
          "job_type" -> params.jobType,
          "start_date" -> params.startDate,
          "end_date" -> params.endDate,
          "layers" -> params.layers,
          "parallelism" -> params.parallelism
        )

        // 发送任务启动通知
        sendNotification("JOB_START", s"数据仓库任务启动: $jobId")
    }

    /**
     * 更新任务进度
     */
    def updateProgress(message: String, progress: Double = -1): Unit = {
        currentMessage = message
        if (progress >= 0) {
            currentProgress = progress
        }

        val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
        logger.info(s"[$timestamp] [$jobId] $message ${if (progress >= 0) s"(${(progress * 100).toInt}%)" else ""}")

        // 记录关键进度节点
        if (message.contains("完成") || message.contains("开始")) {
            metrics += (s"checkpoint_${System.currentTimeMillis()}" -> Map(
                "message" -> message,
                "progress" -> currentProgress,
                "timestamp" -> timestamp
            ))
        }
    }

    /**
     * 完成任务监控
     */
    def completeJob(status: String, errorMessage: Option[String] = None): Unit = {
        currentStatus = status
        val endTime = System.currentTimeMillis()
        val duration = endTime - startTime
        val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))

        logger.info(s"任务监控结束: $jobId")
        logger.info(s"结束时间: $timestamp")
        logger.info(s"任务状态: $status")
        logger.info(s"执行时长: ${formatDuration(duration)}")

        // 更新最终指标
        metrics += (
          "end_time" -> endTime,
          "duration_ms" -> duration,
          "status" -> status,
          "error_message" -> errorMessage.getOrElse("")
        )

        // 生成任务报告
        generateJobReport()

        // 发送任务完成通知
        val notificationType = if (status == "SUCCESS") "JOB_SUCCESS" else "JOB_FAILED"
        val notificationMessage = if (status == "SUCCESS") {
            s"数据仓库任务执行成功: $jobId, 耗时: ${formatDuration(duration)}"
        } else {
            s"数据仓库任务执行失败: $jobId, 错误: ${errorMessage.getOrElse("未知错误")}"
        }
        sendNotification(notificationType, notificationMessage)
    }

    /**
     * 记录性能指标
     */
    def recordMetric(name: String, value: Any): Unit = {
        metrics += (name -> value)
        logger.debug(s"记录指标: $name = $value")
    }

    /**
     * 记录表处理统计
     */
    def recordTableStats(tableName: String, recordCount: Long, processingTime: Long): Unit = {
        val statsKey = s"table_stats_$tableName"
        metrics += (statsKey -> Map(
            "table_name" -> tableName,
            "record_count" -> recordCount,
            "processing_time_ms" -> processingTime,
            "records_per_second" -> (if(processingTime > 0) recordCount * 1000 / processingTime else 0)
        ))

        logger.info(s"表处理统计 - $tableName: 记录数=$recordCount, 耗时=${formatDuration(processingTime)}, " +
          s"处理速度=${if (processingTime > 0) recordCount * 1000 / processingTime else 0} 条/秒")
    }

    /**
     * 检查并发送告警
     */
    def checkAlerts(): Unit = {
        val currentTime = System.currentTimeMillis()
        val runningDuration = currentTime - startTime

        // 检查任务运行时间是否超过阈值
        val maxRunningTime = 2 * 60 * 60 * 1000L // 2小时
        if (runningDuration > maxRunningTime && currentStatus == "RUNNING") {
            val alertMessage = s"任务运行时间过长: $jobId, 已运行 ${formatDuration(runningDuration)}"
            logger.warn(alertMessage)
            sendNotification("ALERT_LONG_RUNNING", alertMessage)
        }

        // 检查内存使用情况
        val runtime = Runtime.getRuntime
        val maxMemory = runtime.maxMemory()
        val totalMemory = runtime.totalMemory()
        val freeMemory = runtime.freeMemory()
        val usedMemory = totalMemory - freeMemory
        val memoryUsagePercent = (usedMemory.toDouble / maxMemory * 100).toInt

        if (memoryUsagePercent > 85) {
            val alertMessage = s"内存使用率过高: $jobId, 使用率: $memoryUsagePercent%"
            logger.warn(alertMessage)
            sendNotification("ALERT_HIGH_MEMORY", alertMessage)
        }

        // 记录当前资源使用情况
        recordMetric("memory_usage_percent", memoryUsagePercent)
        recordMetric("memory_used_mb", usedMemory / 1024 / 1024)
        recordMetric("memory_max_mb", maxMemory / 1024 / 1024)
    }

    /**
     * 发送通知
     */
    private def sendNotification(notificationType: String, message: String): Unit = {
        try {
            // 这里可以集成实际的通知系统，如邮件、短信、钉钉、企业微信等
            logger.info(s"发送通知 [$notificationType]: $message")

            // 示例：发送到钉钉机器人
            // sendToDingTalk(notificationType, message)

            // 示例：发送邮件
            // sendEmail(notificationType, message)

            // 示例：写入告警日志
            val alertLogger = LoggerFactory.getLogger("ALERT")
            alertLogger.info(s"[$notificationType] $message")

        } catch {
            case e: Exception =>
                logger.error(s"发送通知失败: $message", e)
        }
    }

    /**
     * 生成任务报告
     */
    private def generateJobReport(): Unit = {
        try {
            val report = new StringBuilder
            report.append(s"=== 任务执行报告 ===\n")
            report.append(s"任务ID: $jobId\n")
            report.append(s"执行状态: $currentStatus\n")
            report.append(s"开始时间: ${new java.util.Date(startTime)}\n")

            metrics.get("end_time").foreach { endTime =>
                report.append(s"结束时间: ${new java.util.Date(endTime.asInstanceOf[Long])}\n")
            }

            metrics.get("duration_ms").foreach { duration =>
                report.append(s"执行时长: ${formatDuration(duration.asInstanceOf[Long])}\n")
            }

            // 添加表处理统计
            val tableStats = metrics.filter(_._1.startsWith("table_stats_"))
            if (tableStats.nonEmpty) {
                report.append(s"\n=== 表处理统计 ===\n")
                tableStats.foreach { case (key, stats) =>
                    val statsMap = stats.asInstanceOf[Map[String, Any]]
                    report.append(s"表名: ${statsMap("table_name")}\n")
                    report.append(s"  记录数: ${statsMap("record_count")}\n")
                    report.append(s"  处理时间: ${formatDuration(statsMap("processing_time_ms").asInstanceOf[Long])}\n")
                    report.append(s"  处理速度: ${statsMap("records_per_second")} 条/秒\n")
                }
            }

            // 添加检查点信息
            val checkpoints = metrics.filter(_._1.startsWith("checkpoint_"))
            if (checkpoints.nonEmpty) {
                report.append(s"\n=== 执行检查点 ===\n")
                checkpoints.toSeq.sortBy(_._1).foreach { case (_, checkpoint) =>
                    val checkpointMap = checkpoint.asInstanceOf[Map[String, Any]]
                    report.append(s"${checkpointMap("timestamp")}: ${checkpointMap("message")}\n")
                }
            }

            logger.info(s"任务报告:\n${report.toString()}")

            // 可以将报告保存到文件或数据库
            // saveReportToFile(report.toString())

        } catch {
            case e: Exception =>
                logger.error("生成任务报告失败", e)
        }
    }

    /**
     * 格式化时长
     */
    private def formatDuration(durationMs: Long): String = {
        val seconds = durationMs / 1000
        val minutes = seconds / 60
        val hours = minutes / 60
        val days = hours / 24

        if (days > 0) {
            f"${days}天${hours % 24}小时${minutes % 60}分钟${seconds % 60}秒"
        } else if (hours > 0) {
            f"${hours}小时${minutes % 60}分钟${seconds % 60}秒"
        } else if (minutes > 0) {
            f"${minutes}分钟${seconds % 60}秒"
        } else {
            f"${seconds}秒"
        }
    }

    /**
     * 获取当前状态
     */
    def getCurrentStatus: String = currentStatus

    /**
     * 获取当前进度
     */
    def getCurrentProgress: Double = currentProgress

    /**
     * 获取当前消息
     */
    def getCurrentMessage: String = currentMessage

    /**
     * 获取所有指标
     */
    def getMetrics: Map[String, Any] = metrics.toMap

    /**
     * 获取运行时长
     */
    def getRunningDuration: Long = System.currentTimeMillis() - startTime
}
