package org.my_spark.model

case class JobMetrics(
                       applicationId: String,
                       applicationName: String,
                       executorInfos: Int,
                       activeStages: Int,
                       activeJobs: Int
                     )
