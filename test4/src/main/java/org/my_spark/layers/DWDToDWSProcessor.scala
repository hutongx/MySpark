package org.my_spark.layers

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.my_spark.utils.{DataQualityChecker, SparkConfigUtils}
import org.slf4j.{Logger, LoggerFactory}

/**
 * DWD到DWS数据聚合处理器
 * 负责从DWD层读取清洗后的数据，进行聚合统计，生成DWS层主题宽表
 */
class DWDToDWSProcessor(spark: SparkSession) {

    private val logger: Logger = LoggerFactory.getLogger(getClass)
    private val qualityChecker = new DataQualityChecker(spark)

    import spark.implicits._

    /**
     * 构建用户主题宽表
     * @param partitionDate 分区日期
     */
    def buildUserSubjectTable(partitionDate: String): Unit = {
        logger.info(s"开始构建用户主题宽表: $partitionDate")

        try {
            val dwdDB = SparkConfigUtils.getDatabaseName("dwd")
            val dwsDB = SparkConfigUtils.getDatabaseName("dws")

            // 构建用户基础信息和行为统计宽表
            val userWideDF = buildUserWideTable(partitionDate)

            // 数据质量检查
            val qualityResult = qualityChecker.performQualityCheck(
                userWideDF, "dws_user_subject", List("user_id")
            )

            if (qualityResult.passed) {
                // 写入DWS层
                userWideDF.write
                  .mode("overwrite")
                  .partitionBy("dt")
                  .saveAsTable(s"$dwsDB.dws_user_subject")

                logger.info(s"用户主题宽表构建完成: $partitionDate")
            } else {
                throw new RuntimeException(s"用户主题宽表质量检查失败: ${qualityResult.issues}")
            }

        } catch {
            case e: Exception =>
                logger.error(s"构建用户主题宽表失败: $partitionDate", e)
                throw e
        }
    }

    /**
     * 构建商品主题宽表
     * @param partitionDate 分区日期
     */
    def buildProductSubjectTable(partitionDate: String): Unit = {
        logger.info(s"开始构建商品主题宽表: $partitionDate")

        try {
            val dwdDB = SparkConfigUtils.getDatabaseName("dwd")
            val dwsDB = SparkConfigUtils.getDatabaseName("dws")

            // 构建商品销售统计宽表
            val productWideDF = buildProductWideTable(partitionDate)

            // 数据质量检查
            val qualityResult = qualityChecker.performQualityCheck(
                productWideDF, "dws_product_subject", List("product_id")
            )

            if (qualityResult.passed) {
                productWideDF.write
                  .mode("overwrite")
                  .partitionBy("dt")
                  .saveAsTable(s"$dwsDB.dws_product_subject")

                logger.info(s"商品主题宽表构建完成: $partitionDate")
            } else {
                throw new RuntimeException(s"商品主题宽表质量检查失败: ${qualityResult.issues}")
            }

        } catch {
            case e: Exception =>
                logger.error(s"构建商品主题宽表失败: $partitionDate", e)
                throw e
        }
    }

    /**
     * 构建地区主题宽表
     * @param partitionDate 分区日期
     */
    def buildRegionSubjectTable(partitionDate: String): Unit = {
        logger.info(s"开始构建地区主题宽表: $partitionDate")

        try {
            val dwdDB = SparkConfigUtils.getDatabaseName("dwd")
            val dwsDB = SparkConfigUtils.getDatabaseName("dws")

            // 构建地区销售统计宽表
            val regionWideDF = buildRegionWideTable(partitionDate)

            // 数据质量检查
            val qualityResult = qualityChecker.performQualityCheck(
                regionWideDF, "dws_region_subject", List("province", "city")
            )

            if (qualityResult.passed) {
                regionWideDF.write
                  .mode("overwrite")
                  .partitionBy("dt")
                  .saveAsTable(s"$dwsDB.dws_region_subject")

                logger.info(s"地区主题宽表构建完成: $partitionDate")
            } else {
                throw new RuntimeException(s"地区主题宽表质量检查失败: ${qualityResult.issues}")
            }

        } catch {
            case e: Exception =>
                logger.error(s"构建地区主题宽表失败: $partitionDate", e)
                throw e
        }
    }

    /**
     * 构建用户宽表
     */
    private def buildUserWideTable(partitionDate: String): DataFrame = {
        val dwdDB = SparkConfigUtils.getDatabaseName("dwd")

        // 用户基础信息
        val userBaseDF = spark.sql(
            s"""
               |SELECT
               |  user_id,
               |  username,
               |  email,
               |  phone,
               |  gender,
               |  age,
               |  city,
               |  province,
               |  registration_time,
               |  last_login_time,
               |  user_status
               |FROM $dwdDB.dwd_user_info
               |WHERE dt = '$partitionDate'
               |""".stripMargin)

        // 用户订单统计（当天）
        val userOrderStatsDF = spark.sql(
            s"""
               |SELECT
               |  user_id,
               |  COUNT(DISTINCT order_id) as order_count_1d,
               |  SUM(final_amount) as order_amount_1d,
               |  AVG(final_amount) as avg_order_amount_1d,
               |  COUNT(DISTINCT product_id) as product_count_1d,
               |  SUM(quantity) as quantity_1d
               |FROM $dwdDB.dwd_order_info
               |WHERE dt = '$partitionDate' AND order_status = 'completed'
               |GROUP BY user_id
               |""".stripMargin)

        // 用户历史累计统计（7天）
        val userHistoryStats7dDF = spark.sql(
            s"""
               |SELECT
               |  user_id,
               |  COUNT(DISTINCT order_id) as order_count_7d,
               |  SUM(final_amount) as order_amount_7d,
               |  AVG(final_amount) as avg_order_amount_7d,
               |  COUNT(DISTINCT product_id) as product_count_7d,
               |  SUM(quantity) as quantity_7d
               |FROM $dwdDB.dwd_order_info
               |WHERE dt >= date_sub('$partitionDate', 6)
               |  AND dt <= '$partitionDate'
               |  AND order_status = 'completed'
               |GROUP BY user_id
               |""".stripMargin)

        // 用户历史累计统计（30天）
        val userHistoryStats30dDF = spark.sql(
            s"""
               |SELECT
               |  user_id,
               |  COUNT(DISTINCT order_id) as order_count_30d,
               |  SUM(final_amount) as order_amount_30d,
               |  AVG(final_amount) as avg_order_amount_30d,
               |  COUNT(DISTINCT product_id) as product_count_30d,
               |  SUM(quantity) as quantity_30d,
               |  MIN(order_time) as first_order_time_30d,
               |  MAX(order_time) as last_order_time_30d
               |FROM $dwdDB.dwd_order_info
               |WHERE dt >= date_sub('$partitionDate', 29)
               |  AND dt <= '$partitionDate'
               |  AND order_status = 'completed'
               |GROUP BY user_id
               |""".stripMargin)

        // 用户偏好统计（最近30天）
        val userPreferenceDF = spark.sql(
            s"""
               |SELECT
               |  user_id,
               |  first(category_name) as prefer_category_name,
               |  first(brand_name) as prefer_brand_name,
               |  first(payment_method) as prefer_payment_method
               |FROM (
               |  SELECT
               |    user_id,
               |    category_name,
               |    brand_name,
               |    payment_method,
               |    COUNT(*) as cnt,
               |    ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY COUNT(*) DESC) as rn
               |  FROM $dwdDB.dwd_order_info
               |  WHERE dt >= date_sub('$partitionDate', 29)
               |    AND dt <= '$partitionDate'
               |    AND order_status = 'completed'
               |  GROUP BY user_id, category_name, brand_name, payment_method
               |) t
               |WHERE rn = 1
               |GROUP BY user_id
               |""".stripMargin)

        // 关联所有用户统计数据
        val resultDF = userBaseDF
          .join(userOrderStatsDF, Seq("user_id"), "left")
          .join(userHistoryStats7dDF, Seq("user_id"), "left")
          .join(userHistoryStats30dDF, Seq("user_id"), "left")
          .join(userPreferenceDF, Seq("user_id"), "left")
          .select(
              col("user_id"),
              col("username"),
              col("email"),
              col("phone"),
              col("gender"),
              col("age"),
              col("city"),
              col("province"),
              col("registration_time"),
              col("last_login_time"),
              col("user_status"),

              // 当天统计
              coalesce(col("order_count_1d"), lit(0)).as("order_count_1d"),
              coalesce(col("order_amount_1d"), lit(0.0)).cast(DecimalType(15, 2)).as("order_amount_1d"),
              coalesce(col("avg_order_amount_1d"), lit(0.0)).cast(DecimalType(10, 2)).as("avg_order_amount_1d"),
              coalesce(col("product_count_1d"), lit(0)).as("product_count_1d"),
              coalesce(col("quantity_1d"), lit(0)).as("quantity_1d"),

              // 7天统计
              coalesce(col("order_count_7d"), lit(0)).as("order_count_7d"),
              coalesce(col("order_amount_7d"), lit(0.0)).cast(DecimalType(15, 2)).as("order_amount_7d"),
              coalesce(col("avg_order_amount_7d"), lit(0.0)).cast(DecimalType(10, 2)).as("avg_order_amount_7d"),
              coalesce(col("product_count_7d"), lit(0)).as("product_count_7d"),
              coalesce(col("quantity_7d"), lit(0)).as("quantity_7d"),

              // 30天统计
              coalesce(col("order_count_30d"), lit(0)).as("order_count_30d"),
              coalesce(col("order_amount_30d"), lit(0.0)).cast(DecimalType(15, 2)).as("order_amount_30d"),
              coalesce(col("avg_order_amount_30d"), lit(0.0)).cast(DecimalType(10, 2)).as("avg_order_amount_30d"),
              coalesce(col("product_count_30d"), lit(0)).as("product_count_30d"),
              coalesce(col("quantity_30d"), lit(0)).as("quantity_30d"),
              col("first_order_time_30d"),
              col("last_order_time_30d"),

              // 用户偏好
              col("prefer_category_name"),
              col("prefer_brand_name"),
              col("prefer_payment_method"),

              // 用户标签计算
              when(col("order_count_30d") >= 10, "high_frequency")
                .when(col("order_count_30d") >= 3, "medium_frequency")
                .when(col("order_count_30d") >= 1, "low_frequency")
                .otherwise("inactive").as("frequency_label"),

              when(col("order_amount_30d") >= 5000, "high_value")
                .when(col("order_amount_30d") >= 1000, "medium_value")
                .when(col("order_amount_30d") >= 100, "low_value")
                .otherwise("no_purchase").as("value_label"),

              // 计算用户活跃度分数（0-100）
              least(
                  (coalesce(col("order_count_30d"), lit(0)) * 10 +
                    when(col("order_amount_30d") >= 1000, 30)
                      .when(col("order_amount_30d") >= 500, 20)
                      .when(col("order_amount_30d") >= 100, 10)
                      .otherwise(0) +
                    coalesce(col("product_count_30d"), lit(0)) * 2),
                  lit(100)
              ).as("activity_score"),

              // 添加处理时间戳和分区字段
              current_timestamp().as("dws_create_time"),
              lit(partitionDate).as("dt")
          )

        resultDF
    }

    /**
     * 构建商品宽表
     */
    private def buildProductWideTable(partitionDate: String): DataFrame = {
        val dwdDB = SparkConfigUtils.getDatabaseName("dwd")

        // 商品基础信息
        val productBaseDF = spark.sql(
            s"""
               |SELECT
               |  product_id,
               |  product_name,
               |  category_id,
               |  category_name,
               |  brand_id,
               |  brand_name,
               |  price,
               |  cost,
               |  weight,
               |  product_status
               |FROM $dwdDB.dwd_product_info
               |WHERE dt = '$partitionDate'
               |""".stripMargin)

        // 商品销售统计（当天）
        val productSalesStats1dDF = spark.sql(
            s"""
               |SELECT
               |  product_id,
               |  COUNT(DISTINCT order_id) as order_count_1d,
               |  COUNT(DISTINCT user_id) as user_count_1d,
               |  SUM(quantity) as sales_quantity_1d,
               |  SUM(final_amount) as sales_amount_1d,
               |  AVG(final_amount) as avg_order_amount_1d
               |FROM $dwdDB.dwd_order_info
               |WHERE dt = '$partitionDate' AND order_status = 'completed'
               |GROUP BY product_id
               |""".stripMargin)

        // 商品销售统计（7天）
        val productSalesStats7dDF = spark.sql(
            s"""
               |SELECT
               |  product_id,
               |  COUNT(DISTINCT order_id) as order_count_7d,
               |  COUNT(DISTINCT user_id) as user_count_7d,
               |  SUM(quantity) as sales_quantity_7d,
               |  SUM(final_amount) as sales_amount_7d,
               |  AVG(final_amount) as avg_order_amount_7d
               |FROM $dwdDB.dwd_order_info
               |WHERE dt >= date_sub('$partitionDate', 6)
               |  AND dt <= '$partitionDate'
               |  AND order_status = 'completed'
               |GROUP BY product_id
               |""".stripMargin)

        // 商品销售统计（30天）
        val productSalesStats30dDF = spark.sql(
            s"""
               |SELECT
               |  product_id,
               |  COUNT(DISTINCT order_id) as order_count_30d,
               |  COUNT(DISTINCT user_id) as user_count_30d,
               |  SUM(quantity) as sales_quantity_30d,
               |  SUM(final_amount) as sales_amount_30d,
               |  AVG(final_amount) as avg_order_amount_30d,
               |  MIN(order_time) as first_sale_time_30d,
               |  MAX(order_time) as last_sale_time_30d
               |FROM $dwdDB.dwd_order_info
               |WHERE dt >= date_sub('$partitionDate', 29)
               |  AND dt <= '$partitionDate'
               |  AND order_status = 'completed'
               |GROUP BY product_id
               |""".stripMargin)

        // 关联所有商品统计数据
        val resultDF = productBaseDF
          .join(productSalesStats1dDF, Seq("product_id"), "left")
          .join(productSalesStats7dDF, Seq("product_id"), "left")
          .join(productSalesStats30dDF, Seq("product_id"), "left")
          .select(
              col("product_id"),
              col("product_name"),
              col("category_id"),
              col("category_name"),
              col("brand_id"),
              col("brand_name"),
              col("price"),
              col("cost"),
              col("weight"),
              col("product_status"),

              // 当天统计
              coalesce(col("order_count_1d"), lit(0)).as("order_count_1d"),
              coalesce(col("user_count_1d"), lit(0)).as("user_count_1d"),
              coalesce(col("sales_quantity_1d"), lit(0)).as("sales_quantity_1d"),
              coalesce(col("sales_amount_1d"), lit(0.0)).cast(DecimalType(15, 2)).as("sales_amount_1d"),
              coalesce(col("avg_order_amount_1d"), lit(0.0)).cast(DecimalType(10, 2)).as("avg_order_amount_1d"),

              // 7天统计
              coalesce(col("order_count_7d"), lit(0)).as("order_count_7d"),
              coalesce(col("user_count_7d"), lit(0)).as("user_count_7d"),
              coalesce(col("sales_quantity_7d"), lit(0)).as("sales_quantity_7d"),
              coalesce(col("sales_amount_7d"), lit(0.0)).cast(DecimalType(15, 2)).as("sales_amount_7d"),
              coalesce(col("avg_order_amount_7d"), lit(0.0)).cast(DecimalType(10, 2)).as("avg_order_amount_7d"),

              // 30天统计
              coalesce(col("order_count_30d"), lit(0)).as("order_count_30d"),
              coalesce(col("user_count_30d"), lit(0)).as("user_count_30d"),
              coalesce(col("sales_quantity_30d"), lit(0)).as("sales_quantity_30d"),
              coalesce(col("sales_amount_30d"), lit(0.0)).cast(DecimalType(15, 2)).as("sales_amount_30d"),
              coalesce(col("avg_order_amount_30d"), lit(0.0)).cast(DecimalType(10, 2)).as("avg_order_amount_30d"),
              col("first_sale_time_30d"),
              col("last_sale_time_30d"),

              // 商品标签计算
              when(col("sales_quantity_30d") >= 100, "hot")
                .when(col("sales_quantity_30d") >= 10, "normal")
                .when(col("sales_quantity_30d") >= 1, "slow")
                .otherwise("no_sale").as("sales_label"),

              // 计算商品热度分数（0-100）
              least(
                  (coalesce(col("sales_quantity_30d"), lit(0)) +
                    coalesce(col("user_count_30d"), lit(0)) * 2 +
                    when(col("sales_amount_30d") >= 10000, 30)
                      .when(col("sales_amount_30d") >= 5000, 20)
                      .when(col("sales_amount_30d") >= 1000, 10)
                      .otherwise(0)),
                  lit(100)
              ).as("popularity_score"),

              // 添加处理时间戳和分区字段
              current_timestamp().as("dws_create_time"),
              lit(partitionDate).as("dt")
          )

        resultDF
    }

    /**
     * 构建地区宽表
     */
    private def buildRegionWideTable(partitionDate: String): DataFrame = {
        val dwdDB = SparkConfigUtils.getDatabaseName("dwd")

        // 地区销售统计（当天）
        val regionSalesStats1dDF = spark.sql(
            s"""
               |SELECT
               |  province,
               |  city,
               |  COUNT(DISTINCT order_id) as order_count_1d,
               |  COUNT(DISTINCT user_id) as user_count_1d,
               |  COUNT(DISTINCT product_id) as product_count_1d,
               |  SUM(quantity) as sales_quantity_1d,
               |  SUM(final_amount) as sales_amount_1d,
               |  AVG(final_amount) as avg_order_amount_1d
               |FROM $dwdDB.dwd_order_info
               |WHERE dt = '$partitionDate' AND order_status = 'completed'
               |GROUP BY province, city
               |""".stripMargin)

        // 地区销售统计（7天）
        val regionSalesStats7dDF = spark.sql(
            s"""
               |SELECT
               |  province,
               |  city,
               |  COUNT(DISTINCT order_id) as order_count_7d,
               |  COUNT(DISTINCT user_id) as user_count_7d,
               |  COUNT(DISTINCT product_id) as product_count_7d,
               |  SUM(quantity) as sales_quantity_7d,
               |  SUM(final_amount) as sales_amount_7d,
               |  AVG(final_amount) as avg_order_amount_7d
               |FROM $dwdDB.dwd_order_info
               |WHERE dt >= date_sub('$partitionDate', 6)
               |  AND dt <= '$partitionDate'
               |  AND order_status = 'completed'
               |GROUP BY province, city
               |""".stripMargin)

        // 地区销售统计（30天）
        val regionSalesStats30dDF = spark.sql(
            s"""
               |SELECT
               |  province,
               |  city,
               |  COUNT(DISTINCT order_id) as order_count_30d,
               |  COUNT(DISTINCT user_id) as user_count_30d,
               |  COUNT(DISTINCT product_id) as product_count_30d,
               |  SUM(quantity) as sales_quantity_30d,
               |  SUM(final_amount) as sales_amount_30d,
               |  AVG(final_amount) as avg_order_amount_30d,
               |  MIN(order_time) as first_order_time_30d,
               |  MAX(order_time) as last_order_time_30d
               |FROM $dwdDB.dwd_order_info
               |WHERE dt >= date_sub('$partitionDate', 29)
               |  AND dt <= '$partitionDate'
               |  AND order_status = 'completed'
               |GROUP BY province, city
               |""".stripMargin)

        // 关联所有地区统计数据
        val resultDF = regionSalesStats1dDF
          .join(regionSalesStats7dDF, Seq("province", "city"), "full_outer")
          .join(regionSalesStats30dDF, Seq("province", "city"), "full_outer")
          .select(
              coalesce(col("province"), lit("unknown")).as("province"),
              coalesce(col("city"), lit("unknown")).as("city"),

              // 当天统计
              coalesce(col("order_count_1d"), lit(0)).as("order_count_1d"),
              coalesce(col("user_count_1d"), lit(0)).as("user_count_1d"),
              coalesce(col("product_count_1d"), lit(0)).as("product_count_1d"),
              coalesce(col("sales_quantity_1d"), lit(0)).as("sales_quantity_1d"),
              coalesce(col("sales_amount_1d"), lit(0.0)).cast(DecimalType(15, 2)).as("sales_amount_1d"),
              coalesce(col("avg_order_amount_1d"), lit(0.0)).cast(DecimalType(10, 2)).as("avg_order_amount_1d"),

              // 7天统计
              coalesce(col("order_count_7d"), lit(0)).as("order_count_7d"),
              coalesce(col("user_count_7d"), lit(0)).as("user_count_7d"),
              coalesce(col("product_count_7d"), lit(0)).as("product_count_7d"),
              coalesce(col("sales_quantity_7d"), lit(0)).as("sales_quantity_7d"),
              coalesce(col("sales_amount_7d"), lit(0.0)).cast(DecimalType(15, 2)).as("sales_amount_7d"),
              coalesce(col("avg_order_amount_7d"), lit(0.0)).cast(DecimalType(10, 2)).as("avg_order_amount_7d"),

              // 30天统计
              coalesce(col("order_count_30d"), lit(0)).as("order_count_30d"),
              coalesce(col("user_count_30d"), lit(0)).as("user_count_30d"),
              coalesce(col("product_count_30d"), lit(0)).as("product_count_30d"),
              coalesce(col("sales_quantity_30d"), lit(0)).as("sales_quantity_30d"),
              coalesce(col("sales_amount_30d"), lit(0.0)).cast(DecimalType(15, 2)).as("sales_amount_30d"),
              coalesce(col("avg_order_amount_30d"), lit(0.0)).cast(DecimalType(10, 2)).as("avg_order_amount_30d"),
              col("first_order_time_30d"),
              col("last_order_time_30d"),

              // 地区标签计算
              when(col("sales_amount_30d") >= 100000, "tier1")
                .when(col("sales_amount_30d") >= 50000, "tier2")
                .when(col("sales_amount_30d") >= 10000, "tier3")
                .otherwise("tier4").as("region_tier"),

              // 计算地区活跃度分数（0-100）
              least(
                  (coalesce(col("user_count_30d"), lit(0)) * 0.5 +
                    coalesce(col("order_count_30d"), lit(0)) * 0.3 +
                    when(col("sales_amount_30d") >= 50000, 40)
                      .when(col("sales_amount_30d") >= 20000, 30)
                      .when(col("sales_amount_30d") >= 5000, 20)
                      .otherwise(10)),
                  lit(100)
              ).as("activity_score"),

              // 添加处理时间戳和分区字段
              current_timestamp().as("dws_create_time"),
              lit(partitionDate).as("dt")
          )

        resultDF
    }
}
