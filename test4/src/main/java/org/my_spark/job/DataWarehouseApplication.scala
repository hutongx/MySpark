package org.my_spark.job

import org.my_spark.layers.{DWDToDWSProcessor, ODSToDWDProcessor}
import org.my_spark.utils.SparkSessionManager
import org.slf4j.{Logger, LoggerFactory}

/**
 * 数据仓库主应用程序
 * 负责协调整个ETL流程：ODS -> DWD -> DWS
 */
object DataWarehouseApplication {

    private val logger: Logger = LoggerFactory.getLogger(getClass)

    case class Config(
       date: String = "",
       layer: String = "all", // all, dwd, dws
       table: String = "all", // all, user, order, product, region
       appName: String = "DataWarehouseETL"
     )

    def main(args: Array[String]): Unit = {

        val parser = new OptionParser[Config]("DataWarehouseApplication") {
            head("DataWarehouse ETL Application", "1.0")

            opt[String]('d', "date")
              .required()
              .action((x, c) => c.copy(date = x))
              .text("处理日期，格式：yyyy-MM-dd")

            opt[String]('l', "layer")
              .action((x, c) => c.copy(layer = x))
              .text("处理层次：all, dwd, dws，默认：all")

            opt[String]('t', "table")
              .action((x, c) => c.copy(table = x))
              .text("处理表：all, user, order, product, region，默认：all")

            opt[String]('n', "name")
              .action((x, c) => c.copy(appName = x))
              .text("应用名称，默认：DataWarehouseETL")
        }

        parser.parse(args, Config()) match {
            case Some(config) =>
                try {
                    runETL(config)
                } catch {
                    case e: Exception =>
                        logger.error("ETL任务执行失败", e)
                        sys.exit(1)
                }
            case None =>
                logger.error("参数解析失败")
                sys.exit(1)
        }
    }

    /**
     * 执行ETL流程
     */
    def runETL(config: Config): Unit = {
        logger.info(s"开始执行ETL任务: ${config}")

        val spark = SparkSessionManager.createSparkSession()

        try {
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

            logger.info("ETL任务执行完成")

        } finally {
            SparkSessionManager.stopSparkSession(spark)
        }
    }

    /**
     * 执行DWD层处理
     */
    private def runDWDProcess(spark: org.apache.spark.sql.SparkSession, config: Config): Unit = {
        logger.info("开始执行DWD层处理")

        val processor = new ODSToDWDProcessor(spark)

        config.table match {
            case "all" =>
                processor.processUserDimension(config.date)
                processor.processOrderFact(config.date)
                processor.processProductDimension(config.date)
            case "user" =>
                processor.processUserDimension(config.date)
            case "order" =>
                processor.processOrderFact(config.date)
            case "product" =>
                processor.processProductDimension(config.date)
            case _ =>
                throw new IllegalArgumentException(s"DWD层不支持的表类型: ${config.table}")
        }

        logger.info("DWD层处理完成")
    }

    /**
     * 执行DWS层处理
     */
    private def runDWSProcess(spark: org.apache.spark.sql.SparkSession, config: Config): Unit = {
        logger.info("开始执行DWS层处理")

        val processor = new DWDToDWSProcessor(spark)

        config.table match {
            case "all" =>
                processor.buildUserSubjectTable(config.date)
                processor.buildProductSubjectTable(config.date)
                processor.buildRegionSubjectTable(config.date)
            case "user" =>
                processor.buildUserSubjectTable(config.date)
            case "product" =>
                processor.buildProductSubjectTable(config.date)
            case "region" =>
                processor.buildRegionSubjectTable(config.date)
            case _ =>
                throw new IllegalArgumentException(s"DWS层不支持的表类型: ${config.table}")
        }

        logger.info("DWS层处理完成")
    }
}
