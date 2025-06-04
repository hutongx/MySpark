package org.my_spark.model

case class ColumnQualityMetrics(
                                 columnName: String,
                                 totalCount: Long,
                                 nullCount: Long,
                                 distinctCount: Long,
                                 nullPercentage: Double,
                                 uniquenessRatio: Double
                               )

