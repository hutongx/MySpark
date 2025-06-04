package org.my_spark.utils

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

/**
 * Spark Session管理器
 * 负责创建和管理Spark会话，支持配置化管理
 */
object SparkSessionManager {

    private val logger: Logger = LoggerFactory.getLogger(getClass)
    private val config: Config = ConfigFactory.load()

    /**
     * 创建Spark Session
     * @return SparkSession实例
     */
    def createSparkSession(): SparkSession = {
        logger.info("正在创建Spark Session...")

        Try {
            val sparkConfig = config.getConfig("datawarehouse.spark")

            val spark = SparkSession.builder()
                  .appName(sparkConfig.getString("app-name"))
                  .master(sparkConfig.getString("master"))
                  .config("spark.submit.deployMode", sparkConfig.getString("deploy-mode"))
                  .config("spark.executor.instances", sparkConfig.getInt("executor.instances"))
                  .config("spark.executor.cores", sparkConfig.getInt("executor.cores"))
                  .config("spark.executor.memory", sparkConfig.getString("executor.memory"))
                  .config("spark.executor.memoryFraction", sparkConfig.getDouble("executor.memory-fraction"))
                  .config("spark.driver.cores", sparkConfig.getInt("driver.cores"))
                  .config("spark.driver.memory", sparkConfig.getString("driver.memory"))
                  .config("spark.serializer", sparkConfig.getString("serializer"))
                  .config("spark.kryo.registrationRequired", sparkConfig.getBoolean("kryo.registrationRequired"))
                  .config("spark.sql.adaptive.enabled", sparkConfig.getBoolean("sql.adaptive.enabled"))
                  .config("spark.sql.adaptive.coalescePartitions.enabled",
                      sparkConfig.getBoolean("sql.adaptive.coalescePartitions.enabled"))
                  .config("spark.sql.adaptive.skewJoin.enabled",
                      sparkConfig.getBoolean("sql.adaptive.skewJoin.enabled"))
                  .enableHiveSupport()
                  .getOrCreate()

            // 设置Hive相关配置
            val hiveConfig = config.getConfig("datawarehouse.datasource.hive")
            spark.conf.set("hive.metastore.uris", hiveConfig.getString("metastore.uris"))
            spark.conf.set("hive.exec.dynamic.partition", hiveConfig.getBoolean("exec.dynamic.partition"))
            spark.conf.set("hive.exec.dynamic.partition.mode", hiveConfig.getString("exec.dynamic.partition.mode"))
            spark.conf.set("hive.exec.max.dynamic.partitions", hiveConfig.getInt("exec.max.dynamic.partitions"))

            logger.info("Spark Session创建成功")
            spark

        } match {
            case Success(spark) => spark
            case Failure(exception) =>
                logger.error("创建Spark Session失败", exception)
                throw exception
        }
    }

    /**
     * 停止Spark Session
     * @param spark SparkSession实例
     */
    def stopSparkSession(spark: SparkSession): Unit = {
        Try {
            if (spark != null) {
                spark.stop()
                logger.info("Spark Session已停止")
            }
        } match {
            case Success(_) => logger.info("Spark Session停止成功")
            case Failure(exception) => logger.warn("停止Spark Session时出现异常", exception)
        }
    }
}
