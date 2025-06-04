package org.my_spark.job

import org.slf4j.{Logger, LoggerFactory}

/**
 * 调度器启动类
 */
object SchedulerMain {

    private val logger: Logger = LoggerFactory.getLogger(getClass)

    def main(args: Array[String]): Unit = {
        val scheduler = new DataWarehouseScheduler()

        try {
            // 启动定时调度
            scheduler.startScheduledTasks()

            // 注册JVM关闭钩子
            Runtime.getRuntime.addShutdownHook(new Thread(() => {
                logger.info("接收到关闭信号，正在关闭调度器...")
                scheduler.shutdown()
            }))

            logger.info("数据仓库调度器启动成功，等待任务执行...")

            // 保持主线程运行
            while (true) {
                Thread.sleep(60000) // 每分钟检查一次
            }

        } catch {
            case e: Exception =>
                logger.error("调度器启动失败", e)
                scheduler.shutdown()
                sys.exit(1)
        }
    }
}
