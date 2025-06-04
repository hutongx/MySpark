package test2

import org.apache.spark.sql._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import test2.config.ResourceConfig

import scala.util.{Failure, Success, Try}

// ===== RESOURCE MANAGER =====
class ResourceManager(config: ResourceConfig) {

    private val hadoopConf = new Configuration()
    private lazy val hdfsFileSystem = FileSystem.get(hadoopConf)

    def initializeSparkSession(appName: String): SparkSession = {
        val builder = SparkSession.builder()
          .appName(appName)
          .enableHiveSupport()

        // Apply Spark configurations
        config.sparkConfig.foreach { case (key, value) =>
            builder.config(key, value)
        }

        // Apply resource limits
        val limits = config.resourceLimits
        builder
          .config("spark.executor.memory", limits.executorMemory)
          .config("spark.executor.cores", limits.executorCores.toString)
          .config("spark.executor.instances", limits.numExecutors.toString)
          .config("spark.driver.memory", limits.driverMemory)
          .config("spark.driver.maxResultSize", limits.maxResultSize)
          .getOrCreate()
    }

    def createHDFSDirectory(path: String): Boolean = {
        try {
            val hdfsPath = new Path(path)
            if (!hdfsFileSystem.exists(hdfsPath)) {
                hdfsFileSystem.mkdirs(hdfsPath)
                println(s"Created HDFS directory: $path")
                true
            } else {
                println(s"HDFS directory already exists: $path")
                true
            }
        } catch {
            case ex: Exception =>
                println(s"Failed to create HDFS directory $path: ${ex.getMessage}")
                false
        }
    }

    def validateResources(): Boolean = {
        // Check HDFS connectivity
        val hdfsHealthy = Try(hdfsFileSystem.getStatus).isSuccess

        // Check available memory and cores
        val runtime = Runtime.getRuntime
        val availableMemory = runtime.maxMemory() - runtime.totalMemory() + runtime.freeMemory()
        val availableCores = runtime.availableProcessors()

        println(s"Resource Validation:")
        println(s"  HDFS Healthy: $hdfsHealthy")
        println(s"  Available Memory: ${availableMemory / 1024 / 1024} MB")
        println(s"  Available Cores: $availableCores")

        hdfsHealthy
    }
}
