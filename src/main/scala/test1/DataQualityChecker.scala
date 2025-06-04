package test1

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

// ===== DATA QUALITY & MONITORING =====
class DataQualityChecker(spark: SparkSession) {

    def validateDataQuality(tableName: String, partition: String): Boolean = {
        val df = spark.sql(s"SELECT * FROM $tableName WHERE dt = '$partition'")

        val rowCount = df.count()
        val nullCount = df.select(df.columns.map(c => sum(col(c).isNull.cast("int")).alias(c)): _*).collect()(0)

        println(s"Table: $tableName, Partition: $partition")
        println(s"Row Count: $rowCount")
        println(s"Null Counts: $nullCount")

        rowCount > 0 // Basic validation
    }
}
