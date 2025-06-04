package org.my_spark.standard

// 3. SparkSession管理器 - 生产标准
import org.apache.spark.sql.SparkSession
import org.my_spark.standard.config.SparkConfig
import org.slf4j.{Logger, LoggerFactory}

object SparkSessionManager {
    private val logger: Logger = LoggerFactory.getLogger(getClass)
    @volatile private var sparkSession: SparkSession = _

    def getOrCreateSparkSession(sparkConfig: SparkConfig): SparkSession = {
        if (sparkSession == null || sparkSession.sparkContext.isStopped) {
            synchronized {
                if (sparkSession == null || sparkSession.sparkContext.isStopped) {
                    logger.info(s"创建新的SparkSession: ${sparkConfig.appName}")

                    val builder = SparkSession.builder()
                      .appName(sparkConfig.appName)
                      .master(sparkConfig.master)
                      .config("spark.submit.deployMode", sparkConfig.deployMode)

                    // 应用所有配置
                    sparkConfig.sparkConf.foreach { case (key, value) =>
                        builder.config(key, value)
                    }

                    sparkSession = builder.getOrCreate()

                    // 注册JVM关闭钩子
                    sys.addShutdownHook {
                        closeSparkSession()
                    }
                }
            }
        }
        sparkSession
    }

    def closeSparkSession(): Unit = {
        if (sparkSession != null && !sparkSession.sparkContext.isStopped) {
            logger.info("关闭SparkSession")
            sparkSession.close()
            sparkSession = null
        }
    }

    def getCurrentSparkSession: SparkSession = {
        if (sparkSession == null || sparkSession.sparkContext.isStopped) {
            throw new IllegalStateException("SparkSession未初始化或已关闭")
        }
        sparkSession
    }
}

