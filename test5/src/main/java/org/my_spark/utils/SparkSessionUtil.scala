package org.my_spark.utils

import org.apache.spark.sql.SparkSession
import org.my_spark.config.AppConfig

object SparkSessionUtil {

    def getSparkSession(appName: String = AppConfig.sparkAppName): SparkSession = {
        val builder = SparkSession.builder()
          .appName(appName)

        // When running locally, master is usually set in application.conf or directly here.
        // When submitting to YARN, --master yarn is used, and this local setting is ignored.
        if (AppConfig.sparkMaster.startsWith("local")) {
            builder.master(AppConfig.sparkMaster)
        }

        // Apply other Spark configurations from AppConfig
        AppConfig.getSparkConfigs.foreach { case (key, value) =>
            if (!key.equals("spark.master") && !key.equals("spark.appName")) { // Avoid overriding master/appName if set by YARN
                builder.config(key, value)
            }
        }

        // Enable Hive support if needed
        // builder.enableHiveSupport()

        val spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel(AppConfig.sparkLogLevel)

        // Register UDFs globally if any
        // registerUDFs(spark)

        spark
    }

    // Example UDF registration
    // private def registerUDFs(spark: SparkSession): Unit = {
    //   spark.udf.register("exampleUDF", (s: String) => s.toUpperCase())
    // }

    def stopSparkSession(spark: SparkSession): Unit = {
        spark.stop()
    }
}
