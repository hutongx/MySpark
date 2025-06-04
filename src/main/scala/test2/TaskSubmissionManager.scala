package test2

import org.apache.spark.sql._
import test1.processor.{DWDToDWSProcessor, ODSToDWDProcessor}
import test2.config.JobConfig

// ===== TASK SUBMISSION MANAGER =====
class TaskSubmissionManager(spark: SparkSession) {

    def submitETLJob(jobConfig: JobConfig): Boolean = {
        try {
            println(s"Submitting ETL Job: ${jobConfig.jobName}")
            println(s"Parameters: $jobConfig")

            val startTime = System.currentTimeMillis()

            // Execute the job based on configuration
            jobConfig.jobName match {
                case "raw-to-ods" => executeRawToODS(jobConfig)
                case "ods-to-dwd" => executeODSToDWD(jobConfig)
                case "dwd-to-dws" => executeDWDToDWS(jobConfig)
                case "full-pipeline" => executeFullPipeline(jobConfig)
                case _ => throw new IllegalArgumentException(s"Unknown job type: ${jobConfig.jobName}")
            }

            val endTime = System.currentTimeMillis()
            val duration = (endTime - startTime) / 1000.0

            println(s"Job ${jobConfig.jobName} completed successfully in $duration seconds")
            true

        } catch {
            case ex: Exception =>
                println(s"Job ${jobConfig.jobName} failed: ${ex.getMessage}")
                ex.printStackTrace()
                false
        }
    }

    private def executeRawToODS(config: JobConfig): Unit = {
        val importer = new RawDataImporter(spark)
        val df = importer.importFromHDFS(config)
        importer.writeToODS(df, "ods_customer", config.partition)
    }

    private def executeODSToDWD(config: JobConfig): Unit = {
        val processor = new ODSToDWDProcessor(spark)
        processor.processCustomerDimension(config.partition)
        processor.processOrderFact(config.partition)
    }

    private def executeDWDToDWS(config: JobConfig): Unit = {
        val processor = new DWDToDWSProcessor(spark)
        processor.processCustomerSummary(config.partition)
    }

    private def executeFullPipeline(config: JobConfig): Unit = {
        executeRawToODS(config)
        executeODSToDWD(config)
        executeDWDToDWS(config)
    }
}
