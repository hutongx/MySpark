package org.my_spark.config

import java.time.LocalDate

// Case class to hold parsed command-line arguments
case class CliArgs(
                    jobName: String = "", // e.g., "ods-dwd", "dwd-dws", "full-etl"
                    loadDate: LocalDate = LocalDate.now(),
                    sources: Seq[String] = Seq.empty, // Optional: specific sources/tables to process
                    runAllSteps: Boolean = false // Flag to run all steps of a job if applicable
                  )
