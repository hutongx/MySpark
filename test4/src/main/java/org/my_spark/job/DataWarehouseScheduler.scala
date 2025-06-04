package org.my_spark.job

//import com.datawarehouse.DataWarehouseApplication
import org.slf4j.{Logger, LoggerFactory}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

/**
 * 数据仓库调度器
 * 支持定时调度和手动触发ETL任务
 */
class DataWarehouseScheduler {

    private val logger: Logger = LoggerFactory.getLogger(getClass)
    private val scheduler: ScheduledExecutorService = Executors.newScheduledThreadPool(4)
    private val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

    /**
     * 启动定时调度
     * 每天凌晨2点执行前一天的数据处理
     */
    def startScheduledTasks(): Unit = {
        logger.info("启动数据仓库定时调度任务")

        // 每天定时执行全量ETL
        scheduleDailyETL()

        // 每小时执行实时数据同步（如果需要）
        scheduleHourlySync()
    }

    /**
     * 每日ETL调度
     */
    private def scheduleDailyETL(): Unit = {
        val dailyTask = new Runnable {
            override def run(): Unit = {
                try {
                    val yesterday = LocalDate.now().minusDays(1).format(dateFormatter)
                    logger.info(s"开始执行每日ETL任务: $yesterday")

                    val config = DataWarehouseApplication.Config(
                        date = yesterday,
                        layer = "all",
                        table = "all",
                        appName = s"DailyETL_$yesterday"
                    )

                    DataWarehouseApplication.runETL(config)
                    logger.info(s"每日ETL任务完成: $yesterday")

                } catch {
                    case e: Exception =>
                        logger.error("每日ETL任务执行失败", e)
                        // 这里可以添加告警通知逻辑
                        sendAlert("每日ETL任务执行失败", e.getMessage)
                }
            }
        }

        // 每天凌晨2点执行
        scheduler.scheduleAtFixedRate(dailyTask, getInitialDelay(2, 0), 24 * 60 * 60, TimeUnit.SECONDS)
    }

    /**
     * 每小时同步调度（用于实时数据）
     */
    private def scheduleHourlySync(): Unit = {
        val hourlyTask = new Runnable {
            override def run(): Unit = {
                try {
                    val today = LocalDate.now().format(dateFormatter)
                    logger.info(s"开始执行小时级数据同步: $today")

                    // 只处理订单事实表的增量数据
                    val config = DataWarehouseApplication.Config(
                        date = today,
                        layer = "dwd",
                        table = "order",
                        appName = s"HourlySync_$today"
                    )

                    DataWarehouseApplication.runETL(config)
                    logger.info(s"小时级数据同步完成: $today")

                } catch {
                    case e: Exception =>
                        logger.error("小时级数据同步失败", e)
                }
            }
        }

        // 每小时执行一次
        scheduler.scheduleAtFixedRate(hourlyTask, 0, 60 * 60, TimeUnit.SECONDS)
    }

    /**
     * 手动触发ETL任务
     */
    def triggerManualETL(date: String, layer: String = "all", table: String = "all"): Unit = {
        logger.info(s"手动触发ETL任务: date=$date, layer=$layer, table=$table")

        val task = new Runnable {
            override def run(): Unit = {
                try {
                    val config = DataWarehouseApplication.Config(
                        date = date,
                        layer = layer,
                        table = table,
                        appName = s"ManualETL_${date}_${layer}_$table"
                    )

                    DataWarehouseApplication.runETL(config)
                    logger.info(s"手动ETL任务完成: $config")

                } catch {
                    case e: Exception =>
                        logger.error("手动ETL任务执行失败", e)
                        throw e
                }
            }
        }

        scheduler.execute(task)
    }

    /**
     * 批量回刷历史数据
     */
    def backfillHistoricalData(startDate: String, endDate: String, layer: String = "all"): Unit = {
        logger.info(s"开始批量回刷历史数据: $startDate 到 $endDate")

        val start = LocalDate.parse(startDate, dateFormatter)
        val end = LocalDate.parse(endDate, dateFormatter)

        var current = start
        while (!current.isAfter(end)) {
            val dateStr = current.format(dateFormatter)

            val task = new Runnable {
                override def run(): Unit = {
                    try {
                        val config = DataWarehouseApplication.Config(
                            date = dateStr,
                            layer = layer,
                            table = "all",
                            appName = s"Backfill_$dateStr"
                        )

                        DataWarehouseApplication.runETL(config)
                        logger.info(s"历史数据回刷完成: $dateStr")

                    } catch {
                        case e: Exception =>
                            logger.error(s"历史数据回刷失败: $dateStr", e)
                    }
                }
            }

            scheduler.execute(task)
            current = current.plusDays(1)

            // 控制并发，避免资源过载
            Thread.sleep(5000) // 5秒间隔
        }
    }

    /**
     * 停止调度器
     */
    def shutdown(): Unit = {
        logger.info("关闭数据仓库调度器")
        scheduler.shutdown()

        try {
            if (!scheduler.awaitTermination(60, TimeUnit.SECONDS)) {
                scheduler.shutdownNow()
            }
        } catch {
            case _: InterruptedException =>
                scheduler.shutdownNow()
        }
    }

    /**
     * 计算到指定时间的初始延迟（秒）
     */
    private def getInitialDelay(targetHour: Int, targetMinute: Int): Long = {
        val now = java.time.LocalDateTime.now()
        val target = now.toLocalDate.atTime(targetHour, targetMinute)
        val targetTime = if (target.isBefore(now)) target.plusDays(1) else target

        java.time.Duration.between(now, targetTime).getSeconds
    }

    /**
     * 发送告警通知
     */
    private def sendAlert(title: String, message: String): Unit = {
        // 这里可以集成邮件、钉钉、微信等告警通知
        logger.warn(s"告警通知: $title - $message")

        // 示例：发送邮件告警
        // EmailSender.send(title, message)

        // 示例：发送钉钉通知
        // DingTalkSender.send(title, message)
    }
}
