package test1.processor

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// ===== DWD TO DWS PROCESSOR =====
class DWDToDWSProcessor(spark: SparkSession) extends BaseETLProcessor(spark) {

    import test1.model.DataWarehouseMetadata._

    def processCustomerSummary(partition: String): Unit = {
        println(s"Processing Customer Summary for partition: $partition")

        val dwdOrderDF = readHiveTable(dwdOrderFact, partition)
        val dwdCustomerDF = readHiveTable(dwdCustomerDim, partition)

        val customerSummaryDF = dwdOrderDF
          .join(dwdCustomerDF, Seq("customer_sk"), "inner")
          .groupBy("customer_id", "customer_name", "customer_type")
          .agg(
              countDistinct("order_id").as("total_orders"),
              sum("order_amount_usd").as("total_amount_usd"),
              avg("order_amount_usd").as("avg_order_amount"),
              max("order_date").as("last_order_date"),
              min("order_date").as("first_order_date")
          )
          .withColumn("stat_date", lit(partition))
          .withColumn("customer_level",
              when(col("total_amount_usd") >= 10000, "VIP")
                .when(col("total_amount_usd") >= 1000, "Premium")
                .otherwise("Regular"))
          .withColumn("dt", lit(partition))

        val finalDF = addETLColumns(customerSummaryDF)
        writeToHive(finalDF, dwsCustomerSummary, partition)
    }
}
