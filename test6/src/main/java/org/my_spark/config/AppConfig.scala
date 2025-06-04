package org.my_spark.config

import com.typesafe.config.{Config, ConfigFactory}
import scala.util.Try
import scala.collection.JavaConverters._

object AppConfig {
    // Load the default application.conf.
    // You can also allow overriding with system properties or other files:
    // ConfigFactory.systemProperties().withFallback(ConfigFactory.load("application.conf"))
    private val baseConfig: Config = ConfigFactory.load()

    // You can merge with an environment-specific config if needed
    // For example, load 'dev.conf' or 'prod.conf' based on an environment variable
    // val env = Try(sys.env("APP_ENV")).getOrElse("dev")
    // private val config: Config = ConfigFactory.load(s"$env.conf").withFallback(baseConfig)
    private val config: Config = baseConfig // Using base for simplicity here

    // Spark configurations
    val sparkAppName: String = Try(config.getString("spark.appName")).getOrElse("DefaultSparkDWApp")
    val sparkMaster: String = Try(config.getString("spark.master")).getOrElse("local[*]")
    val sparkLogLevel: String = Try(config.getString("spark.logLevel")).getOrElse("WARN")

    def getSparkConfigsMap: Map[String, String] = {
        Try(config.getConfig("spark").entrySet().asScala.map(entry => (s"spark.${entry.getKey}", entry.getValue.unwrapped.toString)).toMap)
          .getOrElse(Map.empty[String, String])
    }

    // Path configurations
    object PathConfig {
        private val paths = config.getConfig("paths")
        def ods(sourceName: String): String = paths.getString(s"ods.$sourceName")
        def dwd(targetName: String): String = paths.getString(s"dwd.$targetName")
        def dws(targetName: String): String = paths.getString(s"dws.$targetName")
        val sqlScriptsBase: String = paths.getString("sql_scripts_base")
    }

    // ETL process configurations
    object EtlConfig {
        private val etl = config.getConfig("etl")
        val defaultLoadDate: String = etl.getString("defaultLoadDate")
        val outputFormat: String = etl.getString("outputFormat")
        val dwdOutputMode: String = etl.getString("dwdOutputMode")
        val dwsOutputMode: String = etl.getString("dwsOutputMode")
    }

    // Helper to get a string with a default
    def getString(path: String, default: String = ""): String = Try(config.getString(path)).getOrElse(default)

    // Helper to get a list of strings
    def getStringList(path: String): List[String] = Try(config.getStringList(path).asScala.toList).getOrElse(List.empty)
}
