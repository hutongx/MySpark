package org.my_spark.model

case class DataFreshnessMetrics(
                                 latestTimestamp: String,
                                 hoursOld: Long
                               )
