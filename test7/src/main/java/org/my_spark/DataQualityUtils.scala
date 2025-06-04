package org.my_spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.logging.log4j.{LogManager, Logger}
import org.my_spark.model.{BusinessRuleViolation, ColumnQualityMetrics, DataFreshnessMetrics, DataQualityReport}

/**
 * Data Quality validation and monitoring utilities
 */
object DataQualityUtils {

    private val logger: Logger = LogManager.getLogger(getClass)

    /**
     * Comprehensive data quality check
     */
    def performDataQualityChecks(df: DataFrame, tableName: String): DataQualityReport = {
        logger.info(s"Performing data quality checks for table: $tableName")

        val totalRecords = df.count()
        val totalColumns = df.columns.length

        // Basic statistics
        val nullCounts = df.columns.map { col =>
            col -> df.filter(df(col).isNull || df(col) === "" || df(col) === "null").count()
        }.toMap

        // Duplicate check
        val duplicateCount = totalRecords - df.dropDuplicates().count()

        // Column-specific validations
        val columnQuality = analyzeColumnQuality(df)

        // Data freshness check (if timestamp column exists)
        val dataFreshness = checkDataFreshness(df)

        val report = DataQualityReport(
            tableName = tableName,
            totalRecords = totalRecords,
            totalColumns = totalColumns,
            nullCounts = nullCounts,
            duplicateCount = duplicateCount,
            columnQuality = columnQuality,
            dataFreshness = dataFreshness,
            qualityScore = calculateQualityScore(totalRecords, nullCounts, duplicateCount)
        )

        logger.info(s"Data quality check completed for $tableName. Quality Score: ${report.qualityScore}%")
        report
    }

    /**
     * Analyze quality metrics for each column
     */
    private def analyzeColumnQuality(df: DataFrame): Map[String, ColumnQualityMetrics] = {
        df.columns.map { colName =>
            val colData = df.select(colName)
            val totalCount = colData.count()
            val nullCount = colData.filter(col(colName).isNull || col(colName) === "").count()
            val distinctCount = colData.distinct().count()

            val metrics = ColumnQualityMetrics(
                columnName = colName,
                totalCount = totalCount,
                nullCount = nullCount,
                distinctCount = distinctCount,
                nullPercentage = if (totalCount > 0) (nullCount.toDouble / totalCount) * 100 else 0.0,
                uniquenessRatio = if (totalCount > 0) distinctCount.toDouble / totalCount else 0.0
            )

            colName -> metrics
        }.toMap
    }

    /**
     * Check data freshness based on timestamp columns
     */
    private def checkDataFreshness(df: DataFrame): Option[DataFreshnessMetrics] = {
        // val timestampColumns = df.columns.filter(_.toLowerCase.contains("time") || _.toLowerCase.contains("date"))
        val timestampColumns = df.columns.filter(col =>
            col.toLowerCase.contains("time") || col.toLowerCase.contains("date"))

        if (timestampColumns.nonEmpty) {
            val latestTimestamp = df.select(max(col(timestampColumns.head))).collect()(0).get(0)
            val currentTime = System.currentTimeMillis()

            Some(DataFreshnessMetrics(
                latestTimestamp = latestTimestamp.toString,
                hoursOld = calculateHoursOld(latestTimestamp.toString, currentTime)
            ))
        } else {
            None
        }
    }

    /**
     * Calculate overall quality score (0-100)
     */
    private def calculateQualityScore(totalRecords: Long, nullCounts: Map[String, Long], duplicateCount: Long): Double = {
        if (totalRecords == 0) return 0.0

        val totalNulls = nullCounts.values.sum
        val nullPenalty = (totalNulls.toDouble / (totalRecords * nullCounts.size)) * 30
        val duplicatePenalty = (duplicateCount.toDouble / totalRecords) * 20

        math.max(0.0, 100.0 - nullPenalty - duplicatePenalty)
    }

    /**
     * Calculate hours between timestamp and current time
     */
    private def calculateHoursOld(timestamp: String, currentTime: Long): Long = {
        // Simplified calculation - implement proper timestamp parsing based on your format
        0L
    }

    /**
     * Validate business rules
     */
    def validateBusinessRules(df: DataFrame, tableName: String): List[BusinessRuleViolation] = {
        logger.info(s"Validating business rules for table: $tableName")

        val violations = scala.collection.mutable.ListBuffer[BusinessRuleViolation]()

        tableName.toLowerCase match {
            case name if name.contains("order") =>
                // Order-specific validations
                violations ++= validateOrderRules(df)
            case name if name.contains("customer") =>
                // Customer-specific validations
                violations ++= validateCustomerRules(df)
            case name if name.contains("product") =>
                // Product-specific validations
                violations ++= validateProductRules(df)
            case _ =>
                logger.info(s"No specific business rules defined for table: $tableName")
        }

        violations.toList
    }

    /**
     * Validate order-specific business rules
     */
    private def validateOrderRules(df: DataFrame): List[BusinessRuleViolation] = {
        val violations = scala.collection.mutable.ListBuffer[BusinessRuleViolation]()

        // Rule 1: Total amount should be positive
        val negativeAmounts = df.filter(col("total_amount") <= 0).count()
        if (negativeAmounts > 0) {
            violations += BusinessRuleViolation("ORDER_001", s"Found $negativeAmounts orders with negative or zero total amount")
        }

        // Rule 2: Quantity should be positive
        val negativeQuantity = df.filter(col("quantity") <= 0).count()
        if (negativeQuantity > 0) {
            violations += BusinessRuleViolation("ORDER_002", s"Found $negativeQuantity orders with negative or zero quantity")
        }

        // Rule 3: Order date should not be in future
        val futureOrders = df.filter(col("order_date") > current_date()).count()
        if (futureOrders > 0) {
            violations += BusinessRuleViolation("ORDER_003", s"Found $futureOrders orders with future dates")
        }

        violations.toList
    }

    /**
     * Validate customer-specific business rules
     */
    private def validateCustomerRules(df: DataFrame): List[BusinessRuleViolation] = {
        val violations = scala.collection.mutable.ListBuffer[BusinessRuleViolation]()

        // Rule 1: Email should be valid format
        val invalidEmails = df.filter(!col("email").rlike("^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$")).count()
        if (invalidEmails > 0) {
            violations += BusinessRuleViolation("CUSTOMER_001", s"Found $invalidEmails customers with invalid email format")
        }

        // Rule 2: Phone should not be null for active customers
        val activeCustomersWithoutPhone = df.filter(col("is_active") === 1 && col("phone").isNull).count()
        if (activeCustomersWithoutPhone > 0) {
            violations += BusinessRuleViolation("CUSTOMER_002", s"Found $activeCustomersWithoutPhone active customers without phone numbers")
        }

        violations.toList
    }

    /**
     * Validate product-specific business rules
     */
    private def validateProductRules(df: DataFrame): List[BusinessRuleViolation] = {
        val violations = scala.collection.mutable.ListBuffer[BusinessRuleViolation]()

        // Rule 1: Price should be greater than cost
        val invalidPricing = df.filter(col("price") <= col("cost")).count()
        if (invalidPricing > 0) {
            violations += BusinessRuleViolation("PRODUCT_001", s"Found $invalidPricing products with price less than or equal to cost")
        }

        // Rule 2: Price should be positive
        val negativePrice = df.filter(col("price") <= 0).count()
        if (negativePrice > 0) {
            violations += BusinessRuleViolation("PRODUCT_002", s"Found $negativePrice products with negative or zero price")
        }

        violations.toList
    }
}
