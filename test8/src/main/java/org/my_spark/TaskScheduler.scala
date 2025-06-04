package org.my_spark

import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
 * 任务调度器
 * 负责协调各层级数据处理任务的执行顺序和依赖关系
 */
class TaskScheduler(spark: SparkSession) {

    private val logger: Logger = LoggerFactory.getLogger(getClass)
    implicit private val ec: ExecutionContext = ExecutionContext.global

    /**
     * 提交DWD层处理任务
     */
    def submitDWDTasks(processor: ODSToDWDProcessor, date: String): Unit = {
        logger.info(s"开始提交DWD层任务: $date")

        try {
            // DWD层任务可以并行执行
            val userTask = Future {
                logger.info(s"处理用户信息: $date")
                processor.processUserDimension(date)
            }

            val productTask = Future {
                logger.info(s"处理商品信息: $date")
                processor.processOrderFact(date)
            }

            val orderTask = Future {
                logger.info(s"处理订单信息: $date")
                processor.processOrderFact(date)
            }

            // 等待所有任务完成
            val allTasks = Future.sequence(Seq(userTask, productTask, orderTask))

            allTasks.onComplete {
                case Success(_) =>
                    logger.info(s"DWD层任务全部完成: $date")
                case Failure(e) =>
                    logger.error(s"DWD层任务执行失败: $date", e)
                    throw e
            }

            // 阻塞等待完成
            scala.concurrent.Await.result(allTasks, scala.concurrent.duration.Duration.Inf)

        } catch {
            case e: Exception =>
                logger.error(s"DWD层任务调度失败: $date", e)
                throw e
        }
    }

    /**
     * 提交DWS层处理任务
     */
    def submitDWSTasks(processor: DWDToDWSProcessor, date: String): Unit = {
        logger.info(s"开始提交DWS层任务: $date")

        try {
            // DWS层任务依赖DWD层，但内部可以并行执行
            val userSubjectTask = Future {
                logger.info(s"构建用户主题宽表: $date")
                processor.buildUserSubjectTable(date)
            }

            val productSubjectTask = Future {
                logger.info(s"构建商品主题宽表: $date")
                processor.buildProductSubjectTable(date)
            }

            val regionSubjectTask = Future {
                logger.info(s"构建地区主题宽表: $date")
                processor.buildRegionSubjectTable(date)
            }

            // 等待所有主题宽表构建完成
            val allTasks = Future.sequence(Seq(userSubjectTask, productSubjectTask, regionSubjectTask))

            allTasks.onComplete {
                case Success(_) =>
                    logger.info(s"DWS层任务全部完成: $date")
                case Failure(e) =>
                    logger.error(s"DWS层任务执行失败: $date", e)
                    throw e
            }

            // 阻塞等待完成
            scala.concurrent.Await.result(allTasks, scala.concurrent.duration.Duration.Inf)

        } catch {
            case e: Exception =>
                logger.error(s"DWS层任务调度失败: $date", e)
                throw e
        }
    }

    /**
     * 提交ADS层处理任务
     */
    def submitADSTasks(processor: DWDToDWSProcessor, date: String): Unit = {
        logger.info(s"开始提交ADS层任务: $date")

        try {
            // ADS层任务依赖DWS层，需要按依赖顺序执行

            // 第一批：基础统计任务（可并行）
            val basicStatsTasks = Future.sequence(Seq(
                Future {
                    logger.info(s"构建用户统计报表: $date")
                    processor.buildUserSubjectTable(date)
                },
                Future {
                    logger.info(s"构建商品统计报表: $date")
                    processor.buildProductSubjectTable(date)
                },
                Future {
                    logger.info(s"构建销售统计报表: $date")
                    processor.buildRegionSubjectTable(date)
                }
            ))

            // 等待基础统计完成
            scala.concurrent.Await.result(basicStatsTasks, scala.concurrent.duration.Duration.Inf)
            logger.info(s"ADS层基础统计任务完成: $date")

            // 第二批：复合分析任务（依赖基础统计）
            val advancedTasks = Future.sequence(Seq(
                Future {
                    logger.info(s"构建用户行为分析: $date")
                    processor.buildUserWideTable(date)
                },
                Future {
                    logger.info(s"构建商品销售趋势分析: $date")
                    processor.buildProductWideTable(date)
                }
            ))

            advancedTasks.onComplete {
                case Success(_) =>
                    logger.info(s"ADS层任务全部完成: $date")
                case Failure(e) =>
                    logger.error(s"ADS层任务执行失败: $date", e)
                    throw e
            }

            // 阻塞等待完成
            scala.concurrent.Await.result(advancedTasks, scala.concurrent.duration.Duration.Inf)

        } catch {
            case e: Exception =>
                logger.error(s"ADS层任务调度失败: $date", e)
                throw e
        }
    }

    /**
     * 检查任务依赖
     */
    def checkDependencies(layer: String, date: String): Boolean = {
        logger.info(s"检查任务依赖: $layer, $date")

        try {
            layer.toLowerCase match {
                case "dwd" => checkODSDependency(date)
                case "dws" => checkDWDDependency(date)
                case "ads" => checkDWSDependency(date)
                case _ => true
            }
        } catch {
            case e: Exception =>
                logger.error(s"依赖检查失败: $layer, $date", e)
                false
        }
    }

    /**
     * 检查ODS层依赖
     */
    private def checkODSDependency(date: String): Boolean = {
        val odsDB = spark.conf.get("spark.sql.warehouse.dir", "default") + ".ods"

        val requiredTables = List("ods_user_info", "ods_product_info", "ods_order_info")

        requiredTables.forall { table =>
            try {
                val count = spark.sql(
                    s"SELECT COUNT(*) as cnt FROM $odsDB.$table WHERE dt = '$date'"
                ).collect()(0).getAs[Long]("cnt")

                if (count > 0) {
                    logger.info(s"ODS表依赖检查通过: $table, 数据量: $count")
                    true
                } else {
                    logger.warn(s"ODS表无数据: $table, 日期: $date")
                    false
                }
            } catch {
                case e: Exception =>
                    logger.error(s"ODS表依赖检查失败: $table", e)
                    false
            }
        }
    }

    /**
     * 检查DWD层依赖
     */
    private def checkDWDDependency(date: String): Boolean = {
        val dwdDB = spark.conf.get("spark.sql.warehouse.dir", "default") + ".dwd"

        val requiredTables = List("dwd_user_info", "dwd_product_info", "dwd_order_info")

        requiredTables.forall { table =>
            try {
                val count = spark.sql(
                    s"SELECT COUNT(*) as cnt FROM $dwdDB.$table WHERE dt = '$date'"
                ).collect()(0).getAs[Long]("cnt")

                if (count > 0) {
                    logger.info(s"DWD表依赖检查通过: $table, 数据量: $count")
                    true
                } else {
                    logger.warn(s"DWD表无数据: $table, 日期: $date")
                    false
                }
            } catch {
                case e: Exception =>
                    logger.error(s"DWD表依赖检查失败: $table", e)
                    false
            }
        }
    }

    /**
     * 检查DWS层依赖
     */
    private def checkDWSDependency(date: String): Boolean = {
        val dwsDB = spark.conf.get("spark.sql.warehouse.dir", "default") + ".dws"

        val requiredTables = List("dws_user_subject", "dws_product_subject", "dws_region_subject")

        requiredTables.forall { table =>
            try {
                val count = spark.sql(
                    s"SELECT COUNT(*) as cnt FROM $dwsDB.$table WHERE dt = '$date'"
                ).collect()(0).getAs[Long]("cnt")

                if (count > 0) {
                    logger.info(s"DWS表依赖检查通过: $table, 数据量: $count")
                    true
                } else {
                    logger.warn(s"DWS表无数据: $table, 日期: $date")
                    false
                }
            } catch {
                case e: Exception =>
                    logger.error(s"DWS表依赖检查失败: $table", e)
                    false
            }
        }
    }

    /**
     * 获取任务执行状态
     */
    def getTaskStatus(taskId: String): Map[String, Any] = {
        // 这里可以集成实际的任务状态管理系统
        Map(
            "task_id" -> taskId,
            "status" -> "RUNNING",
            "start_time" -> System.currentTimeMillis(),
            "progress" -> 0.5
        )
    }
}
