package org.my_spark

import org.apache.spark.sql.SparkSession
import org.my_spark.config.{AppConfig, CliArgs}
import org.my_spark.jobs.{DwdToDwsJob, OdsToDwdJob}
import org.my_spark.utils.SparkSessionUtil
import org.slf4j.{Logger, LoggerFactory}
import scopt.OParser

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object MainApp {
    private val logger: Logger = LoggerFactory.getLogger(this.getClass)

    def main(args: Array[String]): Unit = {
        // --- Argument Parsing using Scopt ---
        val builder = OParser.builder[CliArgs]
        val parser = {
            import builder._
            OParser.sequence(
                programName("SparkDataWarehouseETL"),
                head("Spark Data Warehouse ETL Pipeline", AppConfig.getString("app.version", "1.0.0")),
                opt[String]('j', "job-name")
                  .required()
                  .action((x, c) => c.copy(jobName = x))
                  .text("Name of the job to run (e.g., 'ods-dwd', 'dwd-dws', 'full-etl', 'specific-table-job'). Required."),
                opt[String]('d', "load-date")
                  .optional()
                  .validate(dateStr =>
                      try {
                          LocalDate.parse(dateStr, DateTimeFormatter.ISO_LOCAL_DATE)
                          success
                      } catch {
                          case _: Exception => failure(s"Invalid date format for --load-date. Expected YYYY-MM-DD.")
                      }
                  )
                  .action((x, c) => c.copy(loadDate = LocalDate.parse(x, DateTimeFormatter.ISO_LOCAL_DATE)))
                  .text(s"Load date in YYYY-MM-DD format. Defaults to today or AppConfig.etl.defaultLoadDate (${AppConfig.EtlConfig.defaultLoadDate})."),
                opt[Seq[String]]('s', "sources")
                  .optional()
                  .valueName("<source1>,<source2>...")
                  .action((x, c) => c.copy(sources = x))
                  .text("Comma-separated list of specific sources/tables/topics to process. If empty, processes all defined for the job."),
                opt[Unit]("run-all")
                  .optional()
                  .action((_,c) => c.copy(runAllSteps = true))
                  .text("Flag to run all steps within a job if applicable (e.g. all sources for ods-dwd)."),
                help("help").text("Prints this usage text."),
                note("This application processes data through ODS, DWD, and DWS layers." + sys.props("line.separator"))
            )
        }

        OParser.parse(parser, args, CliArgs(loadDate = LocalDate.parse(AppConfig.EtlConfig.defaultLoadDate))) match {
            case Some(cliArgs) =>
                logger.info(s"Application started with arguments: $cliArgs")
                val effectiveAppName = s"${AppConfig.sparkAppName}-${cliArgs.jobName}-${cliArgs.loadDate}"
                val spark: SparkSession = SparkSessionUtil.getSparkSession(effectiveAppName)

                try {
                    runJob(spark, cliArgs)
                    logger.info(s"Job '${cliArgs.jobName}' for loadDate '${cliArgs.loadDate}' completed successfully.")
                } catch {
                    case e: Exception =>
                        logger.error(s"Application failed for job '${cliArgs.jobName}', loadDate '${cliArgs.loadDate}'. Error: ${e.getMessage}", e)
                        System.exit(1) // Ensure YARN marks the application as FAILED
                } finally {
                    SparkSessionUtil.stopSparkSession(spark)
                    logger.info("Spark session stopped.")
                }

            case None =>
                // Arguments are bad, error message will have been displayed by OParser
                logger.error("Failed to parse command line arguments.")
                System.exit(1)
        }
    }

    private def runJob(spark: SparkSession, cliArgs: CliArgs): Unit = {
        cliArgs.jobName.toLowerCase match {
            case "ods-dwd" =>
                OdsToDwdJob.run(spark, cliArgs)
            case "dwd-dws" =>
                DwdToDwsJob.run(spark, cliArgs)
            case "full-etl" =>
                logger.info("Running full ETL pipeline: ODS to DWD, then DWD to DWS.")
                OdsToDwdJob.run(spark, cliArgs.copy(sources = if(cliArgs.runAllSteps) Seq.empty else cliArgs.sources)) // Process all sources for full ETL if runAllSteps
                DwdToDwsJob.run(spark, cliArgs.copy(sources = if(cliArgs.runAllSteps) Seq.empty else cliArgs.sources)) // Process all topics for full ETL if runAllSteps
            // Add cases for more specific jobs if needed
            // case "process-users-dwd" => OdsToDwdJob.processUsers(spark, cliArgs.loadDate.toString)
            case _ =>
                logger.error(s"Unknown jobName: '${cliArgs.jobName}'. Supported jobs: 'ods-dwd', 'dwd-dws', 'full-etl'.")
                throw new IllegalArgumentException(s"Unknown jobName: ${cliArgs.jobName}")
        }
    }
}
