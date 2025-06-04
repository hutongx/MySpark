package test2.config

import org.apache.spark.sql.SaveMode

case class JobConfig(
  jobName: String,
  partition: String,
  sourceType: String, // HDFS, S3, JDBC, etc.
  sourcePath: String,
  targetFormat: String = "parquet",
  writeMode: SaveMode = SaveMode.Overwrite
)
