package test2


import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import test2.config.JobConfig

// ===== RAW DATA IMPORTER =====
class RawDataImporter(spark: SparkSession) {

    def importFromJDBC(config: JobConfig, jdbcUrl: String, username: String, password: String): DataFrame = {
        println(s"Importing data from JDBC: ${config.sourcePath}")

        val df = spark.read
          .format("jdbc")
          .option("url", jdbcUrl)
          .option("dbtable", config.sourcePath)
          .option("user", username)
          .option("password", password)
          .option("driver", "com.mysql.cj.jdbc.Driver")
          .option("fetchsize", "10000")
          .option("numPartitions", "4")
          .load()

        // Add partition column
        df.withColumn("dt", lit(config.partition))
    }

    def importFromHDFS(config: JobConfig): DataFrame = {
        println(s"Importing data from HDFS: ${config.sourcePath}")

        val df = config.sourceType.toLowerCase match {
            case "csv" =>
                spark.read
                  .option("header", "true")
                  .option("inferSchema", "true")
                  .csv(config.sourcePath)
            case "json" =>
                spark.read.json(config.sourcePath)
            case "parquet" =>
                spark.read.parquet(config.sourcePath)
            case _ =>
                throw new IllegalArgumentException(s"Unsupported source type: ${config.sourceType}")
        }

        df.withColumn("dt", lit(config.partition))
    }

    def importFromS3(config: JobConfig, accessKey: String, secretKey: String): DataFrame = {
        println(s"Importing data from S3: ${config.sourcePath}")

        spark.conf.set("spark.hadoop.fs.s3a.access.key", accessKey)
        spark.conf.set("spark.hadoop.fs.s3a.secret.key", secretKey)

        spark.read.parquet(config.sourcePath)
          .withColumn("dt", lit(config.partition))
    }

    def writeToODS(df: DataFrame, tableName: String, partition: String): Unit = {
        df.coalesce(10) // Optimize file size
          .write
          .mode(SaveMode.Overwrite)
          .partitionBy("dt")
          .saveAsTable(s"ods_db.$tableName")

        println(s"Data written to ODS table: $tableName, partition: $partition")
    }
}
