package config

import org.apache.spark.sql.SparkSession

object SparkConfig {

    def createSparkSession(appName: String): SparkSession = {
        SparkSession.builder()
          .appName(appName)
          .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
          .config("spark.sql.catalogImplementation", "hive")
          .config("hive.metastore.uris", "thrift://localhost:9083")
          .config("spark.sql.adaptive.enabled", "true")
          .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
          .config("spark.sql.adaptive.skewJoin.enabled", "true")
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .config("spark.sql.execution.arrow.pyspark.enabled", "true")
          .enableHiveSupport()
          .getOrCreate()
    }
}