package org.my_spark.utils

import org.apache.spark.sql.SparkSession
import org.my_spark.config.AppConfig
import org.slf4j.{Logger, LoggerFactory}

object SparkSessionUtil {
    private val logger: Logger = LoggerFactory.getLogger(this.getClass)

    def getSparkSession(appName: String = AppConfig.sparkAppName): SparkSession = {
        logger.info(s"Creating SparkSession with appName: $appName")

        val builder = SparkSession.builder().appName(appName)

        // Apply configurations from AppConfig
        // Master URL is typically set by spark-submit on YARN, or from config for local
        if (AppConfig.sparkMaster.toLowerCase.startsWith("local")) {
            builder.master(AppConfig.sparkMaster)
        }

        AppConfig.getSparkConfigsMap.foreach { case (key, value) =>
            // Avoid overriding appName and master if they are meant to be set by YARN or specific logic
            if (!key.equalsIgnoreCase("spark.app.name") && !key.equalsIgnoreCase("spark.master")) {
                logger.debug(s"Setting Spark config: $key -> $value")
                builder.config(key, value)
            }
        }

        // Enable Hive support if needed (and if spark-hive dependency is included)
        // builder.enableHiveSupport()
        // logger.info("Hive support enabled for SparkSession.")

        val spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel(AppConfig.sparkLogLevel)
        logger.info(s"SparkSession created. Spark version: ${spark.version}, Master: ${spark.sparkContext.master}, AppName: ${spark.sparkContext.appName}")

        // Register UDFs globally if you have any
        // registerUDFs(spark)

        spark
    }

    // Example for UDF registration
    // private def registerUDFs(spark: SparkSession): Unit = {
    //   spark.udf.register("customUpper", (s: String) => if (s != null) s.toUpperCase() else null)
    //   logger.info("Registered custom UDFs.")
    // }

    def stopSparkSession(spark: SparkSession): Unit = {
        if (!spark.sparkContext.isStopped) {
            logger.info("Stopping SparkSession.")
            spark.stop()
        }
    }
}

