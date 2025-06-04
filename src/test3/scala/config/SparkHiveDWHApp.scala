package config

import job.PipelineOrchestrator
import org.slf4j.LoggerFactory

object SparkHiveDWHApp {
    private val logger = LoggerFactory.getLogger(this.getClass)

    def main(args: Array[String]): Unit = {
        val spark = AppConfig.createSparkSession()

        try {
            val batchDate = if (args.length > 0) args(0) else getCurrentDate()
            val orchestrator = new PipelineOrchestrator(spark)

            val result = orchestrator.runFullPipeline(batchDate)

            // Exit with appropriate code
            if (result.success) {
                logger.info("Application completed successfully")
                System.exit(0)
            } else {
                logger.error("Application failed")
                System.exit(1)
            }

        } catch {
            case e: Exception =>
                logger.error("Critical application failure", e)
                System.exit(2)
        } finally {
            spark.stop()
        }
    }

    private def getCurrentDate(): String = {
        java.time.LocalDate.now().toString
    }
}
