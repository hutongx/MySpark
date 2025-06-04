package utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

object DataQualityUtils {
    private val logger = LoggerFactory.getLogger(this.getClass)

    /**
     * Validate customer data with comprehensive quality checks
     */
    def validateCustomerData(df: DataFrame): DataFrame = {
        logger.info("Starting customer data validation...")

        val spark = df.sparkSession
        import spark.implicits._

        // Define data quality rules
        val validatedDF = df
          .withColumn("dq_customer_id_valid",
              when(col("customer_id").isNull || col("customer_id") === "", false).otherwise(true))
          .withColumn("dq_customer_name_valid",
              when(col("customer_name").isNull || length(trim(col("customer_name"))) < 2, false).otherwise(true))
          .withColumn("dq_email_valid",
              when(col("email").rlike("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"), true).otherwise(false))
          .withColumn("dq_phone_valid",
              when(col("phone").rlike("^\\+?[1-9]\\d{1,14}$"), true).otherwise(false))
          .withColumn("dq_age_valid",
              when(col("age").between(0, 120), true).otherwise(false))

        // Calculate overall data quality score
        val finalDF = validatedDF
          .withColumn("dq_score",
              (col("dq_customer_id_valid").cast("int") +
                col("dq_customer_name_valid").cast("int") +
                col("dq_email_valid").cast("int") +
                col("dq_phone_valid").cast("int") +
                col("dq_age_valid").cast("int")) / 5.0)
          .withColumn("dq_status",
              when(col("dq_score") >= 0.8, "GOOD")
                .when(col("dq_score") >= 0.6, "FAIR")
                .otherwise("POOR"))

        // Log data quality metrics
        logDataQualityMetrics(finalDF, "customer")

        // Filter out records with poor quality (optional - based on business rules)
        val cleanDF = finalDF.filter(col("dq_score") >= 0.6)

        logger.info(s"Customer data validation completed. Records: ${df.count()} -> ${cleanDF.count()}")
        cleanDF
    }

    /**
     * Validate order data
     */
    def validateOrderData(df: DataFrame): DataFrame = {
        logger.info("Starting order data validation...")

        val spark = df.sparkSession
        import spark.implicits._

        val validatedDF = df
          .withColumn("dq_order_id_valid",
              when(col("order_id").isNull, false).otherwise(true))
          .withColumn("dq_customer_id_valid",
              when(col("customer_id").isNull, false).otherwise(true))
          .withColumn("dq_order_amount_valid",
              when(col("order_amount").isNull || col("order_amount") <= 0, false).otherwise(true))
          .withColumn("dq_order_date_valid",
              when(col("order_date").isNull ||
                col("order_date") < lit("2020-01-01") ||
                col("order_date") > current_date(), false).otherwise(true))
          .withColumn("dq_score",
              (col("dq_order_id_valid").cast("int") +
                col("dq_customer_id_valid").cast("int") +
                col("dq_order_amount_valid").cast("int") +
                col("dq_order_date_valid").cast("int")) / 4.0)
          .withColumn("dq_status",
              when(col("dq_score") >= 0.75, "GOOD")
                .when(col("dq_score") >= 0.5, "FAIR")
                .otherwise("POOR"))

        logDataQualityMetrics(validatedDF, "order")

        val cleanDF = validatedDF.filter(col("dq_score") >= 0.5)
        logger.info(s"Order data validation completed. Records: ${df.count()} -> ${cleanDF.count()}")
        cleanDF
    }

    /**
     * Generic data quality checker for any DataFrame
     */
    def performGenericQualityChecks(df: DataFrame, tableName: String): DataFrame = {
        val spark = df.sparkSession
        import spark.implicits._

        // Check for duplicate records
        val totalRecords = df.count()
        val uniqueRecords = df.distinct().count()
        val duplicateCount = totalRecords - uniqueRecords

        // Check for null values in each column
        val nullCounts = df.columns.map { colName =>
            val nullCount = df.filter(col(colName).isNull).count()
            (colName, nullCount, nullCount.toDouble / totalRecords * 100)
        }

        // Log quality metrics
        logger.info(s"=== Data Quality Report for $tableName ===")
        logger.info(s"Total Records: $totalRecords")
        logger.info(s"Unique Records: $uniqueRecords")
        logger.info(s"Duplicate Records: $duplicateCount")
        logger.info("Null Value Analysis:")
        nullCounts.foreach { case (colName, nullCount, percentage) =>
            logger.info(f"  $colName: $nullCount ($percentage%.2f%%)")
        }

        df
    }

    /**
     * Log data quality metrics to monitoring system
     */
    private def logDataQualityMetrics(df: DataFrame, dataType: String): Unit = {
        val spark = df.sparkSession
        import spark.implicits._

        val qualityMetrics = df
          .groupBy("dq_status")
          .agg(
              count("*").as("record_count"),
              avg("dq_score").as("avg_score")
          )
          .collect()

        logger.info(s"=== Data Quality Metrics for $dataType ===")
        qualityMetrics.foreach { row =>
            val status = row.getString(0)
            val count = row.getLong(1)
            val avgScore = row.getDouble(2)
            logger.info(f"$status: $count records (avg score: $avgScore%.3f)")
        }
    }

    /**
     * Create data quality summary table
     */
    def createQualitySummaryTable(spark: SparkSession, tableName: String,
                                  batchDate: String, metrics: Map[String, Any]): Unit = {
        import spark.implicits._

        val summaryData = Seq(
            (tableName, batchDate,
              metrics.getOrElse("total_records", 0L).asInstanceOf[Long],
              metrics.getOrElse("valid_records", 0L).asInstanceOf[Long],
              metrics.getOrElse("quality_score", 0.0).asInstanceOf[Double],
              java.sql.Timestamp.valueOf(java.time.LocalDateTime.now()))
        ).toDF("table_name", "batch_date", "total_records", "valid_records", "quality_score", "created_at")

        summaryData.write
          .mode("append")
          .saveAsTable("dws.data_quality_summary")
    }
}
