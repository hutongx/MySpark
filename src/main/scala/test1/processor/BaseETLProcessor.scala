package test1.processor

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import test1.model.TableMetadata

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

// ===== BASE ETL FRAMEWORK =====
abstract class BaseETLProcessor(spark: SparkSession) {

    protected def readHiveTable(metadata: TableMetadata, partition: String): DataFrame = {
        spark.sql(s"""
          SELECT * FROM ${metadata.database}.${metadata.tableName}
          WHERE dt = '$partition'
        """)
    }

    protected def writeToHive(df: DataFrame, metadata: TableMetadata,
                              partition: String, mode: SaveMode = SaveMode.Overwrite): Unit = {
        df.write
          .mode(mode)
          .option("path", s"/warehouse/${metadata.database}/${metadata.tableName}")
          .partitionBy(metadata.partitionCols: _*)
          .saveAsTable(s"${metadata.database}.${metadata.tableName}")
    }

    protected def addETLColumns(df: DataFrame, processTime: String = getCurrentTime()): DataFrame = {
        df.withColumn("etl_create_time", lit(processTime))
          .withColumn("etl_update_time", lit(processTime))
          .withColumn("etl_batch_id", lit(java.util.UUID.randomUUID().toString))
    }

    private def getCurrentTime(): String = {
        LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    }
}
