package org.my_spark.layers

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.my_spark.utils.{DataQualityChecker, SparkConfigUtils}
import org.slf4j.{Logger, LoggerFactory}

/**
 * ODS到DWD数据清洗处理器
 * 负责从ODS层读取原始数据，进行清洗转换，生成DWD层数据
 */
class ODSToDWDProcessor(spark: SparkSession) {

    private val logger: Logger = LoggerFactory.getLogger(getClass)
    private val qualityChecker = new DataQualityChecker(spark)

    import spark.implicits._

    /**
     * 处理用户维度数据 ODS -> DWD
     * @param partitionDate 分区日期
     */
    def processUserDimension(partitionDate: String): Unit = {
        logger.info(s"开始处理用户维度数据: $partitionDate")

        try {
            // 读取ODS层用户数据
            val odsUserDF = readODSUserData(partitionDate)

            // 数据清洗和转换
            val dwdUserDF = cleanAndTransformUserData(odsUserDF)

            // 数据质量检查
            val qualityResult = qualityChecker.performQualityCheck(
                dwdUserDF, "dwd_user_info", List("user_id")
            )

            if (qualityResult.passed) {
                // 写入DWD层
                writeDWDUserData(dwdUserDF, partitionDate)
                logger.info(s"用户维度数据处理完成: $partitionDate")
            } else {
                throw new RuntimeException(s"用户维度数据质量检查失败: ${qualityResult.issues}")
            }

        } catch {
            case e: Exception =>
                logger.error(s"处理用户维度数据失败: $partitionDate", e)
                throw e
        }
    }

    /**
     * 处理订单事实数据 ODS -> DWD
     * @param partitionDate 分区日期
     */
    def processOrderFact(partitionDate: String): Unit = {
        logger.info(s"开始处理订单事实数据: $partitionDate")

        try {
            // 读取ODS层订单数据
            val odsOrderDF = readODSOrderData(partitionDate)

            // 数据清洗和转换
            val dwdOrderDF = cleanAndTransformOrderData(odsOrderDF)

            // 数据质量检查
            val qualityResult = qualityChecker.performQualityCheck(
                dwdOrderDF, "dwd_order_info", List("order_id")
            )

            if (qualityResult.passed) {
                // 写入DWD层
                writeDWDOrderData(dwdOrderDF, partitionDate)
                logger.info(s"订单事实数据处理完成: $partitionDate")
            } else {
                throw new RuntimeException(s"订单事实数据质量检查失败: ${qualityResult.issues}")
            }

        } catch {
            case e: Exception =>
                logger.error(s"处理订单事实数据失败: $partitionDate", e)
                throw e
        }
    }

    /**
     * 读取ODS层用户数据
     */
    private def readODSUserData(partitionDate: String): DataFrame = {
        val odsDB = SparkConfigUtils.getDatabaseName("ods")

        spark.sql(
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
               |  user_status,
               |  create_time,
               |  update_time
               |FROM $odsDB.ods_user_info
               |WHERE dt = '$partitionDate'
               |""".stripMargin)
    }

    /**
     * 清洗和转换用户数据
     */
    private def cleanAndTransformUserData(df: DataFrame): DataFrame = {
        df.select(
            col("user_id").cast(LongType).as("user_id"),

            // 用户名清洗：去除空格，空值处理
            when(trim(col("username")).isNull || trim(col("username")) === "", "unknown_user")
              .otherwise(trim(col("username"))).as("username"),

            // 邮箱清洗：格式校验
            when(col("email").rlike("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"), col("email"))
              .otherwise(null).as("email"),

            // 手机号清洗：格式校验
            when(col("phone").rlike("^1[3-9]\\d{9}$"), col("phone"))
              .otherwise(null).as("phone"),

            // 性别标准化
            when(upper(col("gender")) === "M" || upper(col("gender")) === "MALE", "M")
              .when(upper(col("gender")) === "F" || upper(col("gender")) === "FEMALE", "F")
              .otherwise("U").as("gender"),

            // 年龄清洗：范围校验
            when(col("age").isNotNull && col("age") >= 0 && col("age") <= 120, col("age"))
              .otherwise(null).as("age"),

            // 地区信息清洗
            when(trim(col("city")).isNull || trim(col("city")) === "", "unknown")
              .otherwise(trim(col("city"))).as("city"),
            when(trim(col("province")).isNull || trim(col("province")) === "", "unknown")
              .otherwise(trim(col("province"))).as("province"),

            // 时间字段转换
            to_timestamp(col("registration_time"), "yyyy-MM-dd HH:mm:ss").as("registration_time"),
            to_timestamp(col("last_login_time"), "yyyy-MM-dd HH:mm:ss").as("last_login_time"),

            // 状态标准化
            when(col("user_status") === "1" || upper(col("user_status")) === "ACTIVE", "active")
              .when(col("user_status") === "0" || upper(col("user_status")) === "INACTIVE", "inactive")
              .otherwise("unknown").as("user_status"),

            // 添加数据处理时间戳
            current_timestamp().as("dwd_create_time"),
            current_timestamp().as("dwd_update_time"),

            // 添加数据版本号（用于SCD处理）
            lit(1).as("version"),

            // 添加有效标识
            lit(true).as("is_active")
        )
    }

    /**
     * 读取ODS层订单数据
     */
    private def readODSOrderData(partitionDate: String): DataFrame = {
        val odsDB = SparkConfigUtils.getDatabaseName("ods")

        spark.sql(
            s"""
               |SELECT
               |  order_id,
               |  user_id,
               |  product_id,
               |  product_name,
               |  category_id,
               |  category_name,
               |  brand_id,
               |  brand_name,
               |  order_time,
               |  pay_time,
               |  finish_time,
               |  order_status,
               |  payment_method,
               |  quantity,
               |  unit_price,
               |  total_amount,
               |  discount_amount,
               |  final_amount,
               |  province,
               |  city,
               |  create_time,
               |  update_time
               |FROM $odsDB.ods_order_info
               |WHERE dt = '$partitionDate'
               |""".stripMargin)
    }

    /**
     * 清洗和转换订单数据
     */
    private def cleanAndTransformOrderData(df: DataFrame): DataFrame = {
        df.select(
            col("order_id").cast(StringType).as("order_id"),
            col("user_id").cast(LongType).as("user_id"),
            col("product_id").cast(StringType).as("product_id"),

            // 商品名称清洗
            when(trim(col("product_name")).isNull || trim(col("product_name")) === "", "unknown_product")
              .otherwise(trim(col("product_name"))).as("product_name"),

            col("category_id").cast(LongType).as("category_id"),
            when(trim(col("category_name")).isNull || trim(col("category_name")) === "", "unknown_category")
              .otherwise(trim(col("category_name"))).as("category_name"),

            col("brand_id").cast(LongType).as("brand_id"),
            when(trim(col("brand_name")).isNull || trim(col("brand_name")) === "", "unknown_brand")
              .otherwise(trim(col("brand_name"))).as("brand_name"),

            // 时间字段处理
            to_timestamp(col("order_time"), "yyyy-MM-dd HH:mm:ss").as("order_time"),
            to_timestamp(col("pay_time"), "yyyy-MM-dd HH:mm:ss").as("pay_time"),
            to_timestamp(col("finish_time"), "yyyy-MM-dd HH:mm:ss").as("finish_time"),

            // 订单状态标准化
            when(col("order_status") === "1", "created")
              .when(col("order_status") === "2", "paid")
              .when(col("order_status") === "3", "shipped")
              .when(col("order_status") === "4", "completed")
              .when(col("order_status") === "5", "cancelled")
              .otherwise("unknown").as("order_status"),

            // 支付方式标准化
            when(upper(col("payment_method")) === "ALIPAY", "alipay")
              .when(upper(col("payment_method")) === "WECHAT", "wechat")
              .when(upper(col("payment_method")) === "UNIONPAY", "unionpay")
              .when(upper(col("payment_method")) === "CASH", "cash")
              .otherwise("other").as("payment_method"),

            // 数量和金额清洗：确保非负数
            when(col("quantity").isNotNull && col("quantity") > 0, col("quantity"))
              .otherwise(0).cast(IntegerType).as("quantity"),

            when(col("unit_price").isNotNull && col("unit_price") >= 0, col("unit_price"))
              .otherwise(0.0).cast(DecimalType(10, 2)).as("unit_price"),

            when(col("total_amount").isNotNull && col("total_amount") >= 0, col("total_amount"))
              .otherwise(0.0).cast(DecimalType(10, 2)).as("total_amount"),

            when(col("discount_amount").isNotNull && col("discount_amount") >= 0, col("discount_amount"))
              .otherwise(0.0).cast(DecimalType(10, 2)).as("discount_amount"),

            when(col("final_amount").isNotNull && col("final_amount") >= 0, col("final_amount"))
              .otherwise(0.0).cast(DecimalType(10, 2)).as("final_amount"),

            // 地区信息
            when(trim(col("province")).isNull || trim(col("province")) === "", "unknown")
              .otherwise(trim(col("province"))).as("province"),
            when(trim(col("city")).isNull || trim(col("city")) === "", "unknown")
              .otherwise(trim(col("city"))).as("city"),

            // 添加处理时间戳
            current_timestamp().as("dwd_create_time"),
            current_timestamp().as("dwd_update_time")

        ).filter(
            // 过滤掉无效数据
            col("order_id").isNotNull && col("user_id").isNotNull && col("product_id").isNotNull
        )
    }

    /**
     * 写入DWD层用户数据
     */
    private def writeDWDUserData(df: DataFrame, partitionDate: String): Unit = {
        val dwdDB = SparkConfigUtils.getDatabaseName("dwd")

        df.write
          .mode("overwrite")
          .option("hive.exec.dynamic.partition", "true")
          .option("hive.exec.dynamic.partition.mode", "nonstrict")
          .partitionBy("dt")
          .saveAsTable(s"$dwdDB.dwd_user_info")

        logger.info(s"DWD用户数据写入完成: $dwdDB.dwd_user_info, 分区: $partitionDate")
    }

    /**
     * 写入DWD层订单数据
     */
    private def writeDWDOrderData(df: DataFrame, partitionDate: String): Unit = {
        val dwdDB = SparkConfigUtils.getDatabaseName("dwd")

        df.withColumn("dt", lit(partitionDate))
          .write
          .mode("overwrite")
          .option("hive.exec.dynamic.partition", "true")
          .option("hive.exec.dynamic.partition.mode", "nonstrict")
          .partitionBy("dt")
          .saveAsTable(s"$dwdDB.dwd_order_info")

        logger.info(s"DWD订单数据写入完成: $dwdDB.dwd_order_info, 分区: $partitionDate")
    }

    /**
     * 处理商品维度数据 ODS -> DWD
     */
    def processProductDimension(partitionDate: String): Unit = {
        logger.info(s"开始处理商品维度数据: $partitionDate")

        try {
            val odsDB = SparkConfigUtils.getDatabaseName("ods")
            val dwdDB = SparkConfigUtils.getDatabaseName("dwd")

            // 读取并清洗商品数据
            val odsProductDF = spark.sql(
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
                   |  dimensions,
                   |  description,
                   |  product_status,
                   |  create_time,
                   |  update_time
                   |FROM $odsDB.ods_product_info
                   |WHERE dt = '$partitionDate'
                   |""".stripMargin)

            val dwdProductDF = odsProductDF.select(
                col("product_id").cast(StringType).as("product_id"),

                when(trim(col("product_name")).isNull || trim(col("product_name")) === "", "unknown_product")
                  .otherwise(trim(col("product_name"))).as("product_name"),

                col("category_id").cast(LongType).as("category_id"),
                when(trim(col("category_name")).isNull || trim(col("category_name")) === "", "unknown_category")
                  .otherwise(trim(col("category_name"))).as("category_name"),

                col("brand_id").cast(LongType).as("brand_id"),
                when(trim(col("brand_name")).isNull || trim(col("brand_name")) === "", "unknown_brand")
                  .otherwise(trim(col("brand_name"))).as("brand_name"),

                when(col("price").isNotNull && col("price") >= 0, col("price"))
                  .otherwise(0.0).cast(DecimalType(10, 2)).as("price"),

                when(col("cost").isNotNull && col("cost") >= 0, col("cost"))
                  .otherwise(0.0).cast(DecimalType(10, 2)).as("cost"),

                col("weight").cast(DecimalType(8, 2)).as("weight"),
                col("dimensions").as("dimensions"),
                col("description").as("description"),

                when(col("product_status") === "1", "active")
                  .when(col("product_status") === "0", "inactive")
                  .otherwise("unknown").as("product_status"),

                current_timestamp().as("dwd_create_time"),
                current_timestamp().as("dwd_update_time")
            )

            // 数据质量检查
            val qualityResult = qualityChecker.performQualityCheck(
                dwdProductDF, "dwd_product_info", List("product_id")
            )

            if (qualityResult.passed) {
                dwdProductDF.withColumn("dt", lit(partitionDate))
                  .write
                  .mode("overwrite")
                  .partitionBy("dt")
                  .saveAsTable(s"$dwdDB.dwd_product_info")

                logger.info(s"商品维度数据处理完成: $partitionDate")
            } else {
                throw new RuntimeException(s"商品维度数据质量检查失败: ${qualityResult.issues}")
            }

        } catch {
            case e: Exception =>
                logger.error(s"处理商品维度数据失败: $partitionDate", e)
                throw e
        }
    }
}
