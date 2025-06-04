package org.my_spark.standard

// 4. 主应用程序 - 生产标准
import org.apache.spark.sql.SparkSession
import org.my_spark.layers.{DWDToDWSProcessor, ODSToDWDProcessor}
import org.my_spark.standard.config.ETLConfig
import org.slf4j.{Logger, LoggerFactory}

object DataWarehouseApplication {
    private val logger: Logger = LoggerFactory.getLogger(getClass)

    def main(args: Array[String]): Unit = {
        try {
            // 1. 加载配置
            val sparkConfig = ConfigManager.getSparkConfig
            val etlConfig = ConfigManager.getETLConfig

            // 2. 创建SparkSession
            val spark = SparkSessionManager.getOrCreateSparkSession(sparkConfig)

            // 3. 执行ETL
            runETL(spark, etlConfig)

            logger.info("ETL任务执行完成")

        } catch {
            case e: Exception =>
                logger.error("ETL任务执行失败", e)
                sys.exit(1)
        } finally {
            // SparkSession会在JVM关闭时自动关闭
        }
    }

    private def runETL(spark: SparkSession, config: ETLConfig): Unit = {
        logger.info(s"开始执行ETL任务: layer=${config.layer}, table=${config.table}, date=${config.date}")

        config.layer match {
            case "all" =>
                runDWDProcess(spark, config)
                runDWSProcess(spark, config)
            case "dwd" =>
                runDWDProcess(spark, config)
            case "dws" =>
                runDWSProcess(spark, config)
            case _ =>
                throw new IllegalArgumentException(s"不支持的处理层次: ${config.layer}")
        }
    }

    private def runDWDProcess(spark: SparkSession, config: ETLConfig): Unit = {
        logger.info("开始执行DWD层处理")
        val processor = new ODSToDWDProcessor(spark)

        config.table match {
            case "all" =>
                processor.processUserDimension(config.date)
                processor.processOrderFact(config.date)
                processor.processProductDimension(config.date)
            case "user" => processor.processUserDimension(config.date)
            case "order" => processor.processOrderFact(config.date)
            case "product" => processor.processProductDimension(config.date)
            case _ => throw new IllegalArgumentException(s"DWD层不支持的表类型: ${config.table}")
        }

        logger.info("DWD层处理完成")
    }

    private def runDWSProcess(spark: SparkSession, config: ETLConfig): Unit = {
        logger.info("开始执行DWS层处理")
        val processor = new DWDToDWSProcessor(spark)

        config.table match {
            case "all" =>
                processor.buildUserSubjectTable(config.date)
                processor.buildProductSubjectTable(config.date)
                processor.buildRegionSubjectTable(config.date)
            case "user" => processor.buildUserSubjectTable(config.date)
            case "product" => processor.buildProductSubjectTable(config.date)
            case "region" => processor.buildRegionSubjectTable(config.date)
            case _ => throw new IllegalArgumentException(s"DWS层不支持的表类型: ${config.table}")
        }

        logger.info("DWS层处理完成")
    }
}
