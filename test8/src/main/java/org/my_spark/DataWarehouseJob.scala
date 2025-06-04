package org.my_spark

import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
import scopt.OptionParser

/**
 * 数据仓库主任务入口
 * 支持全量和增量数据处理，支持数据修复
 */
object DataWarehouseJob {

    private val logger: Logger = LoggerFactory.getLogger(getClass)

    case class JobParams(
                          jobType: String = "",
                          startDate: String = "",
                          endDate: String = "",
                          layers: String = "all",
                          isRepair: Boolean = false,
                          parallelism: Int = 4
                        )

    def main(args: Array[String]): Unit = {
        val parser = new OptionParser[JobParams]("DataWarehouseJob") {
            head("数据仓库任务", "1.0")

            opt[String]('t', "job-type")
              .required()
              .action((x, c) => c.copy(jobType = x))
              .text("任务类型: daily(日常增量), repair(数据修复), full(全量重跑)")

            opt[String]('s', "start-date")
              .required()
              .action((x, c) => c.copy(startDate = x))
              .text("开始日期 (yyyy-MM-dd)")

            opt[String]('e', "end-date")
              .action((x, c) => c.copy(endDate = x))
              .text("结束日期 (yyyy-MM-dd), 不指定则处理单天")

            opt[String]('l', "layers")
              .action((x, c) => c.copy(layers = x))
              .text("处理层级: ods,dwd,dws,ads 或 all (默认all)")

            opt[Unit]('r', "repair")
              .action((_, c) => c.copy(isRepair = true))
              .text("是否为修复任务")

            opt[Int]('p', "parallelism")
              .action((x, c) => c.copy(parallelism = x))
              .text("并行度 (默认4)")
        }

        parser.parse(args, JobParams()) match {
            case Some(params) =>
                executeJob(params)
            case None =>
                System.exit(1)
        }
    }

    /**
     * 执行数据仓库任务
     */
    def executeJob(params: JobParams): Unit = {
        val jobId = s"dw_job_${System.currentTimeMillis()}"
        val monitor = new JobMonitor(jobId)

        try {
            logger.info(s"开始执行数据仓库任务: $jobId")
            logger.info(s"任务参数: $params")

            // 初始化Spark会话
            val spark = createSparkSession(params)

            // 启动任务监控
            monitor.startJob(params)

            // 获取处理日期列表
            val processDates = getProcessDates(params)
            logger.info(s"需要处理的日期: ${processDates.mkString(", ")}")

            // 根据任务类型执行不同的处理逻辑
            params.jobType match {
                case "daily" => executeDailyJob(spark, processDates, params, monitor)
                case "repair" => executeRepairJob(spark, processDates, params, monitor)
                case "full" => executeFullJob(spark, processDates, params, monitor)
                case _ => throw new IllegalArgumentException(s"不支持的任务类型: ${params.jobType}")
            }

            monitor.completeJob("SUCCESS")
            logger.info(s"数据仓库任务执行成功: $jobId")

        } catch {
            case e: Exception =>
                logger.error(s"数据仓库任务执行失败: $jobId", e)
                monitor.completeJob("FAILED", Some(e.getMessage))
                throw e
        } finally {
            SparkSession.getActiveSession.foreach(_.stop())
        }
    }

    /**
     * 创建Spark会话
     */
    private def createSparkSession(params: JobParams): SparkSession = {
        val appName = s"DataWarehouse-${params.jobType}-${params.startDate}"

        SparkSession.builder()
          .appName(appName)
          .config("spark.sql.adaptive.enabled", "true")
          .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
          .config("spark.sql.adaptive.skewJoin.enabled", "true")
          .config("spark.sql.execution.arrow.pyspark.enabled", "true")
          .config("spark.default.parallelism", params.parallelism * 2)
          .config("spark.sql.shuffle.partitions", params.parallelism * 4)
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .config("spark.sql.hive.convertMetastoreParquet", "true")
          .config("spark.sql.parquet.compression.codec", "snappy")
          .enableHiveSupport()
          .getOrCreate()
    }

    /**
     * 执行日常增量任务
     */
    private def executeDailyJob(
                                 spark: SparkSession,
                                 dates: List[String],
                                 params: JobParams,
                                 monitor: JobMonitor
                               ): Unit = {

        val scheduler = new TaskScheduler(spark)
        val layers = parseLayers(params.layers)

        dates.foreach { date =>
            logger.info(s"开始处理日期: $date")
            monitor.updateProgress(s"Processing date: $date")

            try {
                // 按层级顺序处理
                if (layers.contains("ods")) {
                    logger.info(s"处理ODS层: $date")
                    // ODS层通常由外部ETL工具处理，这里可以做数据验证
                    validateODSData(spark, date)
                }

                if (layers.contains("dwd")) {
                    logger.info(s"处理DWD层: $date")
                    val processor = new ODSToDWDProcessor(spark)
                    scheduler.submitDWDTasks(processor, date)
                }

                if (layers.contains("dws")) {
                    logger.info(s"处理DWS层: $date")
                    val processor = new DWDToDWSProcessor(spark)
                    scheduler.submitDWSTasks(processor, date)
                }

                if (layers.contains("ads")) {
                    logger.info(s"处理ADS层: $date")
                    val processor = new DWDToDWSProcessor(spark)
                    scheduler.submitADSTasks(processor, date)
                }

                logger.info(s"日期处理完成: $date")

            } catch {
                case e: Exception =>
                    logger.error(s"处理日期失败: $date", e)
                    throw e
            }
        }
    }

    /**
     * 执行修复任务
     */
    private def executeRepairJob(
                                  spark: SparkSession,
                                  dates: List[String],
                                  params: JobParams,
                                  monitor: JobMonitor
                                ): Unit = {

        logger.info("执行数据修复任务")
        val layers = parseLayers(params.layers)

        // 修复任务通常需要先清理目标数据
        dates.foreach { date =>
            logger.info(s"修复日期: $date")
            monitor.updateProgress(s"Repairing date: $date")

            if (layers.contains("dwd")) {
                val processor = new DWDToDWSProcessor(spark)
                processor.repairData(date, date)
            }

            if (layers.contains("dws")) {
                val processor = new DWDToDWSProcessor(spark)
                processor.repairData(date, date)
            }

            if (layers.contains("ads")) {
                val processor = new DWDToDWSProcessor(spark)
                processor.repairData(date, date)
            }
        }

        logger.info("数据修复任务完成")
    }

    /**
     * 执行全量重跑任务
     */
    private def executeFullJob(
                                spark: SparkSession,
                                dates: List[String],
                                params: JobParams,
                                monitor: JobMonitor
                              ): Unit = {

        logger.info("执行全量重跑任务")

        // 全量重跑通常需要清理所有目标表
        clearTargetTables(spark, params.layers)

        // 然后按日期顺序重新处理
        executeDailyJob(spark, dates, params, monitor)

        logger.info("全量重跑任务完成")
    }

    /**
     * 获取需要处理的日期列表
     */
    private def getProcessDates(params: JobParams): List[String] = {
        import java.time.LocalDate
        import java.time.format.DateTimeFormatter

        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        val startDate = LocalDate.parse(params.startDate, formatter)
        val endDate = if (params.endDate.nonEmpty) {
            LocalDate.parse(params.endDate, formatter)
        } else {
            startDate
        }

        var dates = List[String]()
        var current = startDate

        while (!current.isAfter(endDate)) {
            dates = dates :+ current.format(formatter)
            current = current.plusDays(1)
        }

        dates
    }

    /**
     * 解析需要处理的层级
     */
    private def parseLayers(layersStr: String): Set[String] = {
        if (layersStr == "all") {
            Set("ods", "dwd", "dws", "ads")
        } else {
            layersStr.split(",").map(_.trim.toLowerCase).toSet
        }
    }

    /**
     * 验证ODS数据
     */
    private def validateODSData(spark: SparkSession, date: String): Unit = {
        val odsDB = SparkConfigUtils.getDatabaseName("ods")

        val tables = List("ods_user_info", "ods_product_info", "ods_order_info")

        tables.foreach { table =>
            val count = spark.sql(
                s"SELECT COUNT(*) as cnt FROM $odsDB.$table WHERE dt = '$date'"
            ).collect()(0).getAs[Long]("cnt")

            logger.info(s"ODS表 $table 日期 $date 数据量: $count")

            if (count == 0) {
                logger.warn(s"ODS表 $table 日期 $date 无数据")
            }
        }
    }

    /**
     * 清理目标表
     */
    private def clearTargetTables(spark: SparkSession, layers: String): Unit = {
        val targetLayers = parseLayers(layers)

        if (targetLayers.contains("dwd")) {
            logger.info("清理DWD层表")
            val dwdDB = SparkConfigUtils.getDatabaseName("dwd")
            spark.sql(s"DROP TABLE IF EXISTS $dwdDB.dwd_user_info")
            spark.sql(s"DROP TABLE IF EXISTS $dwdDB.dwd_product_info")
            spark.sql(s"DROP TABLE IF EXISTS $dwdDB.dwd_order_info")
        }

        if (targetLayers.contains("dws")) {
            logger.info("清理DWS层表")
            val dwsDB = SparkConfigUtils.getDatabaseName("dws")
            spark.sql(s"DROP TABLE IF EXISTS $dwsDB.dws_user_subject")
            spark.sql(s"DROP TABLE IF EXISTS $dwsDB.dws_product_subject")
            spark.sql(s"DROP TABLE IF EXISTS $dwsDB.dws_region_subject")
        }

        if (targetLayers.contains("ads")) {
            logger.info("清理ADS层表")
            val adsDB = SparkConfigUtils.getDatabaseName("ads")
            spark.sql(s"DROP TABLE IF EXISTS $adsDB.ads_user_stats")
            spark.sql(s"DROP TABLE IF EXISTS $adsDB.ads_product_stats")
            spark.sql(s"DROP TABLE IF EXISTS $adsDB.ads_sales_stats")
        }
    }
}
