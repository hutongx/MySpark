package org.my_spark.standard

import com.typesafe.config.{Config, ConfigFactory}
import org.my_spark.standard.config.{ETLConfig, SparkConfig}

import scala.collection.JavaConverters._

object ConfigManager {
    private val config: Config = ConfigFactory.load()

    def getSparkConfig: SparkConfig = {
        val sparkConfig = config.getConfig("spark")
        SparkConfig(
            appName = sparkConfig.getString("app-name"),
            master = sparkConfig.getString("master"),
            deployMode = sparkConfig.getString("deploy-mode"),
            sparkConf = sparkConfig.getConfig("conf").entrySet().asScala.map { entry =>
                entry.getKey -> entry.getValue.unwrapped().toString
            }.toMap
        )
    }

    def getETLConfig: ETLConfig = {
        val etlConfig = config.getConfig("etl")
        ETLConfig(
            date = etlConfig.getString("date"),
            layer = etlConfig.getString("layer"),
            table = etlConfig.getString("table")
        )
    }
}
