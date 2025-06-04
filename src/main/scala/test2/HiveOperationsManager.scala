package test2

import org.apache.spark.sql._

// ===== HIVE OPERATIONS MANAGER =====
class HiveOperationsManager(spark: SparkSession) {

    def optimizeTable(database: String, tableName: String): Unit = {
        // Analyze table statistics
        spark.sql(s"ANALYZE TABLE $database.$tableName COMPUTE STATISTICS")
        spark.sql(s"ANALYZE TABLE $database.$tableName COMPUTE STATISTICS FOR COLUMNS")
        println(s"Table statistics updated for $database.$tableName")
    }

    def repairTable(database: String, tableName: String): Unit = {
        spark.sql(s"MSCK REPAIR TABLE $database.$tableName")
        println(s"Table repaired: $database.$tableName")
    }

    def compactSmallFiles(database: String, tableName: String, partition: String): Unit = {
        val tempTable = s"${tableName}_temp_${System.currentTimeMillis()}"

        // Read and rewrite to compact files
        spark.sql(s"""
          CREATE TABLE $database.$tempTable
          STORED AS PARQUET AS
          SELECT * FROM $database.$tableName WHERE dt = '$partition'
        """)

        // Replace original partition
        spark.sql(s"ALTER TABLE $database.$tableName DROP PARTITION (dt='$partition')")
        spark.sql(s"""
          INSERT INTO $database.$tableName PARTITION (dt='$partition')
          SELECT * FROM $database.$tempTable
        """)

        spark.sql(s"DROP TABLE $database.$tempTable")
        println(s"Small files compacted for $database.$tableName, partition: $partition")
    }

    def getTableInfo(database: String, tableName: String): Unit = {
        val info = spark.sql(s"DESCRIBE EXTENDED $database.$tableName").collect()
        println(s"Table Info for $database.$tableName:")
        info.foreach(row => println(s"  ${row.getString(0)}: ${row.getString(1)}"))
    }
}
