package test1.model

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

// ===== METADATA CONFIGURATION =====
case class TableMetadata(
      tableName: String,
      database: String,
      partitionCols: Seq[String],
      primaryKeys: Seq[String],
      businessKeys: Seq[String],
      etlTimestamp: String = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
)
