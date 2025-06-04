package test1

import org.apache.spark.sql.SparkSession
import test1.processor.{DWDToDWSProcessor, ODSToDWDProcessor}

// ===== MAIN PIPELINE ORCHESTRATOR =====
class DataWarehousePipeline(spark: SparkSession) {

    private val odsToDwd = new ODSToDWDProcessor(spark)
    private val dwdToDws = new DWDToDWSProcessor(spark)

    def runFullPipeline(partition: String): Unit = {
        try {
            println(s"Starting Data Warehouse Pipeline for partition: $partition")

            // Step 1: ODS to DWD
            println("=== ODS to DWD Processing ===")
            odsToDwd.processCustomerDimension(partition)
            odsToDwd.processOrderFact(partition)

            // Step 2: DWD to DWS
            println("=== DWD to DWS Processing ===")
            dwdToDws.processCustomerSummary(partition)

            println(s"Pipeline completed successfully for partition: $partition")

        } catch {
            case ex: Exception =>
                println(s"Pipeline failed for partition $partition: ${ex.getMessage}")
                ex.printStackTrace()
                throw ex
        }
    }

    def runIncrementalPipeline(startDate: String, endDate: String): Unit = {
        val dates = generateDateRange(startDate, endDate)
        dates.foreach(date => runFullPipeline(date))
    }

    private def generateDateRange(start: String, end: String): Seq[String] = {
        // Implementation for date range generation
        Seq(start) // Simplified - implement full date range logic
    }
}
