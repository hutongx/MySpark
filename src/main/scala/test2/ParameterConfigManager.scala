package test2

import test2.config.ResourceConfig
import test2.config.ResourceLimits

import java.util.Properties
import scala.util.{Failure, Success, Try}

// ===== PARAMETER CONFIGURATION MANAGER =====
class ParameterConfigManager {

    def loadConfiguration(configPath: String): Properties = {
        val props = new Properties()

        // Default configurations
        props.setProperty("spark.app.name", "DataWarehouse-Pipeline")
        props.setProperty("spark.executor.memory", "4g")
        props.setProperty("spark.executor.cores", "2")
        props.setProperty("spark.executor.instances", "10")
        props.setProperty("spark.driver.memory", "2g")
        props.setProperty("spark.sql.adaptive.enabled", "true")
        props.setProperty("spark.sql.adaptive.coalescePartitions.enabled", "true")
        props.setProperty("spark.sql.adaptive.skewJoin.enabled", "true")
        props.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        props.setProperty("spark.sql.parquet.compression.codec", "snappy")
        props.setProperty("hive.exec.dynamic.partition", "true")
        props.setProperty("hive.exec.dynamic.partition.mode", "nonstrict")
        props.setProperty("hive.exec.max.dynamic.partitions", "10000")

        // Load external configuration if exists
        Try {
            val inputStream = getClass.getClassLoader.getResourceAsStream(configPath)
            if (inputStream != null) {
                props.load(inputStream)
                inputStream.close()
            }
        } match {
            case Success(_) => println(s"Configuration loaded from: $configPath")
            case Failure(ex) => println(s"Using default configuration. Failed to load: $configPath")
        }

        props
    }

    def createResourceConfig(props: Properties): ResourceConfig = {
        val sparkConfig = Map(
            "spark.sql.adaptive.enabled" -> props.getProperty("spark.sql.adaptive.enabled"),
            "spark.sql.adaptive.coalescePartitions.enabled" -> props.getProperty("spark.sql.adaptive.coalescePartitions.enabled"),
            "spark.sql.adaptive.skewJoin.enabled" -> props.getProperty("spark.sql.adaptive.skewJoin.enabled"),
            "spark.serializer" -> props.getProperty("spark.serializer"),
            "spark.sql.parquet.compression.codec" -> props.getProperty("spark.sql.parquet.compression.codec")
        )

        val hiveConfig = Map(
            "hive.exec.dynamic.partition" -> props.getProperty("hive.exec.dynamic.partition"),
            "hive.exec.dynamic.partition.mode" -> props.getProperty("hive.exec.dynamic.partition.mode"),
            "hive.exec.max.dynamic.partitions" -> props.getProperty("hive.exec.max.dynamic.partitions")
        )

        val resourceLimits = ResourceLimits(
            executorMemory = props.getProperty("spark.executor.memory"),
            executorCores = props.getProperty("spark.executor.cores").toInt,
            numExecutors = props.getProperty("spark.executor.instances").toInt,
            driverMemory = props.getProperty("spark.driver.memory")
        )

        ResourceConfig(sparkConfig, hiveConfig, Map.empty, resourceLimits)
    }
}
