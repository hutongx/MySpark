package org.my_spark.utils

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

/**
 * 数据质量检查器
 * 提供数据完整性、一致性、准确性检查功能
 */
class DataQualityChecker(spark: SparkSession) {

    private val logger: Logger = LoggerFactory.getLogger(getClass)
    private val config: Config = ConfigFactory.load()

    /**
     * 数据质量检查结果
     */
    case class QualityCheckResult(
       tableName: String,
       totalRows: Long,
       nullRows: Long,
       nullRatio: Double,
       duplicateRows: Long,
       duplicateRatio: Double,
       passed: Boolean,
       issues: List[String]
     )

    /**
     * 执行完整的数据质量检查
     * @param df 待检查的DataFrame
     * @param tableName 表名
     * @param keyColumns 主键列
     * @return 检查结果
     */
    def performQualityCheck(df: DataFrame, tableName: String, keyColumns: List[String]): QualityCheckResult = {
        logger.info(s"开始对表 $tableName 进行数据质量检查")

        val totalRows = df.count()
        val issues = scala.collection.mutable.ListBuffer[String]()

        // 空值检查
        val nullCheckResult = checkNullValues(df, tableName)
        val nullRows = nullCheckResult._1
        val nullRatio = nullCheckResult._2

        if (nullRatio > config.getDouble("datawarehouse.quality.null-check-threshold")) {
            issues += s"空值比例过高: $nullRatio"
        }

        // 重复值检查
        val duplicateCheckResult = if (config.getBoolean("datawarehouse.quality.duplicate-check-enabled")) {
            checkDuplicates(df, keyColumns)
        } else {
            (0L, 0.0)
        }
        val duplicateRows = duplicateCheckResult._1
        val duplicateRatio = duplicateCheckResult._2

        if (duplicateRatio > 0) {
            issues += s"存在重复数据: $duplicateRows 行"
        }

        // 数据范围检查
        performRangeCheck(df, tableName).foreach(issue => issues += issue)

        // 数据格式检查
        performFormatCheck(df, tableName).foreach(issue => issues += issue)

        val passed = issues.isEmpty
        val result = QualityCheckResult(
            tableName = tableName,
            totalRows = totalRows,
            nullRows = nullRows,
            nullRatio = nullRatio,
            duplicateRows = duplicateRows,
            duplicateRatio = duplicateRatio,
            passed = passed,
            issues = issues.toList
        )

        logQualityCheckResult(result)
        result
    }

    /**
     * 检查空值
     */
    private def checkNullValues(df: DataFrame, tableName: String): (Long, Double) = {
        val totalRows = df.count()
        if (totalRows == 0) return (0L, 0.0)

        val nullCounts = df.columns.map { col =>
            df.filter(df(col).isNull || df(col) === "" || df(col) === "null").count()
        }

        val totalNulls = nullCounts.sum
        val nullRatio = totalNulls.toDouble / (totalRows * df.columns.length)

        logger.info(s"表 $tableName 空值检查: 总行数=$totalRows, 空值数=$totalNulls, 空值比例=$nullRatio")
        (totalNulls, nullRatio)
    }

    /**
     * 检查重复值
     */
    private def checkDuplicates(df: DataFrame, keyColumns: List[String]): (Long, Double) = {
        if (keyColumns.isEmpty) return (0L, 0.0)

        val totalRows = df.count()
        if (totalRows == 0) return (0L, 0.0)

        val distinctRows = df.dropDuplicates(keyColumns).count()
        val duplicateRows = totalRows - distinctRows
        val duplicateRatio = duplicateRows.toDouble / totalRows

        logger.info(s"重复值检查: 总行数=$totalRows, 去重后=$distinctRows, 重复行数=$duplicateRows")
        (duplicateRows, duplicateRatio)
    }

    /**
     * 数据范围检查
     */
    private def performRangeCheck(df: DataFrame, tableName: String): List[String] = {
        val issues = scala.collection.mutable.ListBuffer[String]()

        // 检查数值型字段的范围
        val numericColumns = df.dtypes.filter(_._2.contains("Int") || _._2.contains("Double") || _._2.contains("Float")).map(_._1)

        numericColumns.foreach { col =>
            val stats = df.select(
                min(col).alias("min_val"),
                max(col).alias("max_val"),
                avg(col).alias("avg_val")
            ).collect().head

            val minVal = stats.getAs[Any]("min_val")
            val maxVal = stats.getAs[Any]("max_val")

            // 检查是否有异常的极值
            if (minVal != null && maxVal != null) {
                logger.debug(s"字段 $col 范围: min=$minVal, max=$maxVal")
            }
        }

        issues.toList
    }

    /**
     * 数据格式检查
     */
    private def performFormatCheck(df: DataFrame, tableName: String): List[String] = {
        val issues = scala.collection.mutable.ListBuffer[String]()

        // 检查日期格式
        val dateColumns = df.dtypes.filter(_._2.contains("Date") || _._2.contains("Timestamp")).map(_._1)

        dateColumns.foreach { col =>
            val invalidDates = df.filter(df(col).isNull).count()
            if (invalidDates > 0) {
                issues += s"字段 $col 存在 $invalidDates 个无效日期"
            }
        }

        issues.toList
    }

    /**
     * 日志记录检查结果
     */
    private def logQualityCheckResult(result: QualityCheckResult): Unit = {
        logger.info(s"=== 数据质量检查结果: ${result.tableName} ===")
        logger.info(s"总行数: ${result.totalRows}")
        logger.info(s"空值行数: ${result.nullRows} (${result.nullRatio * 100}%)")
        logger.info(s"重复行数: ${result.duplicateRows} (${result.duplicateRatio * 100}%)")
        logger.info(s"检查通过: ${result.passed}")

        if (result.issues.nonEmpty) {
            logger.warn("发现的问题:")
            result.issues.foreach(issue => logger.warn(s"  - $issue"))
        }
    }

    /**
     * 保存质量检查结果到表
     */
    def saveQualityCheckResult(result: QualityCheckResult, partitionDate: String): Unit = {
        import spark.implicits._

        val resultDF = Seq(
            (result.tableName, result.totalRows, result.nullRows, result.nullRatio,
              result.duplicateRows, result.duplicateRatio, result.passed,
              result.issues.mkString(";"), partitionDate)
        ).toDF("table_name", "total_rows", "null_rows", "null_ratio",
            "duplicate_rows", "duplicate_ratio", "passed", "issues", "check_date")

        resultDF.write
          .mode("append")
          .partitionBy("check_date")
          .saveAsTable("dws_db.data_quality_check_result")

        logger.info(s"质量检查结果已保存到表 dws_db.data_quality_check_result")
    }
}
