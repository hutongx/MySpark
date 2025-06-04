package config

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession

object AppConfig {
    private val config: Config = ConfigFactory.load()

    def createSparkSession(): SparkSession = {
        SparkSession.builder()
          .appName(config.getString("spark.app-name"))
          .config("spark.sql.warehouse.dir", config.getString("spark.sql.warehouse.dir"))
          .config("spark.sql.catalogImplementation", config.getString("spark.sql.catalogImplementation"))
          .config("hive.metastore.uris", config.getString("spark.hive.metastore.uris"))
          .config("spark.sql.adaptive.enabled", config.getBoolean("spark.sql.adaptive.enabled"))
          .config("spark.sql.adaptive.coalescePartitions.enabled",
              config.getBoolean("spark.sql.adaptive.coalescePartitions.enabled"))
          .config("spark.sql.adaptive.skewJoin.enabled",
              config.getBoolean("spark.sql.adaptive.skewJoin.enabled"))
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .enableHiveSupport()
          .getOrCreate()
    }

    def getDatabaseConfig(configPath: String): DatabaseConfig = {
        val dbConfig = config.getConfig(configPath)
        DatabaseConfig(
            url = dbConfig.getString("url"),
            user = dbConfig.getString("user"),
            password = dbConfig.getString("password"),
            driver = dbConfig.getString("driver")
        )
    }

    def getDataQualityThreshold(dataType: String): Double = {
        config.getDouble(s"data-quality.$dataType.min-score")
    }
}

case class DatabaseConfig(url: String, user: String, password: String, driver: String)
