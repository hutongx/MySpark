package job


import layers.{DWDProcessor, DWSProcessor, ODSProcessor}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import utils.{DataQualityUtils, MonitoringUtils}

import scala.util.Try

class PipelineOrchestrator(spark: SparkSession) {
    private val logger = LoggerFactory.getLogger(this.getClass)

    def runFullPipeline(batchDate: String): PipelineResult = {
        val startTime = System.currentTimeMillis()
        var pipelineResult = PipelineResult(success = true, errors = List())

        try {
            logger.info(s"Starting full pipeline execution for $batchDate")

            // Step 1: ODS Processing
            val odsResult = runODSLayer(batchDate)
            pipelineResult = pipelineResult.copy(
                odsSuccess = odsResult.isSuccess,
                errors = pipelineResult.errors ++ odsResult.failed.map(_.getMessage).toList
            )

            if (odsResult.isSuccess) {
                // Step 2: DWD Processing
                val dwdResult = runDWDLayer(batchDate)
                pipelineResult = pipelineResult.copy(
                    dwdSuccess = dwdResult.isSuccess,
                    errors = pipelineResult.errors ++ dwdResult.failed.map(_.getMessage).toList
                )

                if (dwdResult.isSuccess) {
                    // Step 3: DWS Processing
                    val dwsResult = runDWSLayer(batchDate)
                    pipelineResult = pipelineResult.copy(
                        dwsSuccess = dwsResult.isSuccess,
                        errors = pipelineResult.errors ++ dwsResult.failed.map(_.getMessage).toList
                    )
                }
            }

            val endTime = System.currentTimeMillis()
            val duration = endTime - startTime

            pipelineResult = pipelineResult.copy(
                success = pipelineResult.odsSuccess && pipelineResult.dwdSuccess && pipelineResult.dwsSuccess,
                executionTimeMs = duration
            )

            // Log final result
            if (pipelineResult.success) {
                logger.info(s"Pipeline completed successfully in ${duration}ms")
            } else {
                logger.error(s"Pipeline failed. Errors: ${pipelineResult.errors.mkString(", ")}")
            }

            // Send monitoring metrics
            MonitoringUtils.sendPipelineMetrics(pipelineResult, batchDate)

        } catch {
            case e: Exception =>
                logger.error("Critical pipeline failure", e)
                pipelineResult = pipelineResult.copy(
                    success = false,
                    errors = pipelineResult.errors :+ e.getMessage
                )
        }

        pipelineResult
    }

    private def runODSLayer(batchDate: String): Try[Unit] = {
        Try {
            val odsProcessor = new ODSProcessor(spark)
            odsProcessor.processCustomerData(batchDate)
            odsProcessor.processOrderData(batchDate)

            // Data quality validation
            val customerDF = spark.table("ods.customers").filter(s"etl_date = '$batchDate'")
            DataQualityUtils.performGenericQualityChecks(customerDF, "ods.customers")
        }
    }

    private def runDWDLayer(batchDate: String): Try[Unit] = {
        Try {
            val dwdProcessor = new DWDProcessor(spark)
            dwdProcessor.processCustomerDWD(batchDate)
            dwdProcessor.processOrderDWD(batchDate)
        }
    }

    private def runDWSLayer(batchDate: String): Try[Unit] = {
        Try {
            val dwsProcessor = new DWSProcessor(spark)
            dwsProcessor.generateCustomerSummary(batchDate)
            dwsProcessor.generateOrderStatistics(batchDate)
        }
    }
}

case class PipelineResult(
    success: Boolean,
    odsSuccess: Boolean = false,
    dwdSuccess: Boolean = false,
    dwsSuccess: Boolean = false,
    errors: List[String] = List(),
    executionTimeMs: Long = 0L
)
