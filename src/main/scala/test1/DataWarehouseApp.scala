package test1

import org.apache.spark.sql.SparkSession

// ===== APPLICATION ENTRY POINT =====
object DataWarehouseApp {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder()
          .appName("DataWarehouse-Pipeline")
          .config("spark.sql.adaptive.enabled", "true")
          .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
          .config("spark.sql.adaptive.skewJoin.enabled", "true")
          .enableHiveSupport()
          .getOrCreate()

        try {
            val pipeline = new DataWarehousePipeline(spark)

            val partition = args.headOption.getOrElse("2024-01-01")

            pipeline.runFullPipeline(partition)

        } finally {
            spark.stop()
        }
    }
}
