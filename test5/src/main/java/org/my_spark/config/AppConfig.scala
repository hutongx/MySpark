package org.my_spark.config

import com.typesafe.config.{Config, ConfigFactory}
import scala.util.Try

object AppConfig {
    private val config: Config = ConfigFactory.load("application.conf") // Loads application.conf from classpath

    // Spark configurations
    val sparkAppName: String = Try(config.getString("spark.appName")).getOrElse("DefaultSparkDWApp")
    val sparkMaster: String = Try(config.getString("spark.master")).getOrElse("local[*]")
    val sparkLogLevel: String = Try(config.getString("spark.logLevel")).getOrElse("WARN")

    // Path configurations
    def getOdsPath(sourceName: String): String = config.getString(s"paths.ods.$sourceName")
    def getDwdPath(tableName: String): String = config.getString(s"paths.dwd.$tableName")
    def getDwsPath(tableName: String): String = config.getString(s"paths.dws.$tableName")

    def getSqlQueriesPath(layer: String): String = config.getString(s"paths.sql_queries.$layer")

    // Processing configurations
    val defaultLoadDate: String = Try(config.getString("processing.defaultLoadDate")).getOrElse("1970-01-01")
    val outputFormat: String = Try(config.getString("processing.outputFormat")).getOrElse("parquet")
    val outputMode: String = Try(config.getString("processing.outputMode")).getOrElse("overwrite")

    // You can add more specific configurations as needed
    // For example, to get all Spark configurations:
    def getSparkConfigs: Map[String, String] = {
        import scala.collection.JavaConverters._
        Try(config.getConfig("spark").entrySet().asScala.map(entry => (s"spark.${entry.getKey}", entry.getValue.unwrapped.toString)).toMap)
          .getOrElse(Map.empty[String, String])
    }
}