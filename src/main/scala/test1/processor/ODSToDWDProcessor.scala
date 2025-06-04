package test1.processor

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// ===== ODS TO DWD PROCESSOR =====
class ODSToDWDProcessor(spark: SparkSession) extends BaseETLProcessor(spark) {

    import test1.model.DataWarehouseMetadata._

    def processCustomerDimension(partition: String): Unit = {
        println(s"Processing Customer Dimension for partition: $partition")

        val odsCustomerDF = readHiveTable(odsCustomer, partition)

        val dwdCustomerDF = odsCustomerDF
          .withColumn("customer_sk", monotonically_increasing_id())
          .withColumn("effective_date", col("dt"))
          .withColumn("expiry_date", lit("9999-12-31"))
          .withColumn("is_current", lit(1))
          .withColumn("data_source", lit("ODS_CUSTOMER"))
          .select(
              col("customer_sk"),
              col("customer_id"),
              col("customer_code"),
              col("customer_name"),
              col("customer_type"),
              col("registration_date"),
              col("status"),
              col("effective_date"),
              col("expiry_date"),
              col("is_current"),
              col("data_source"),
              col("dt")
          )

        val finalDF = addETLColumns(dwdCustomerDF)
        writeToHive(finalDF, dwdCustomerDim, partition)
    }

    def processOrderFact(partition: String): Unit = {
        println(s"Processing Order Fact for partition: $partition")

        val odsOrderDF = readHiveTable(odsOrder, partition)
        val dwdCustomerDF = readHiveTable(dwdCustomerDim, partition)

        val dwdOrderDF = odsOrderDF
          .join(dwdCustomerDF, Seq("customer_id"), "left")
          .withColumn("order_sk", monotonically_increasing_id())
          .withColumn("order_amount_usd",
              when(col("currency") === "CNY", col("order_amount") / 7.0)
                .otherwise(col("order_amount")))
          .select(
              col("order_sk"),
              col("order_id"),
              col("order_no"),
              col("customer_sk"),
              col("customer_id"),
              col("order_date"),
              col("order_amount"),
              col("order_amount_usd"),
              col("currency"),
              col("order_status"),
              col("payment_method"),
              col("dt")
          )

        val finalDF = addETLColumns(dwdOrderDF)
        writeToHive(finalDF, dwdOrderFact, partition)
    }
}
