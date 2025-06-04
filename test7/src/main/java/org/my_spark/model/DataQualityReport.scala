package org.my_spark.model

// Case classes for data structures
case class DataQualityReport(
                              tableName: String,
                              totalRecords: Long,
                              totalColumns: Int,
                              nullCounts: Map[String, Long],
                              duplicateCount: Long,
                              columnQuality: Map[String, ColumnQualityMetrics],
                              dataFreshness: Option[DataFreshnessMetrics],
                              qualityScore: Double
                            )
