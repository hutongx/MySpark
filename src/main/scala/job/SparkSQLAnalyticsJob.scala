package job

import com.sun.org.slf4j.internal.LoggerFactory
import sink.MultiSinkManager
import source.DataSourceManager
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

object SparkSQLAnalyticsJob {

    private val LOG = LoggerFactory.getLogger(getClass)
    private var spark: SparkSession = _
    private var sourceMgr: DataSourceManager = _
    private var sinkMgr: MultiSinkManager = _
    private val queries = scala.collection.mutable.Map.empty[String, StreamingQuery]

    def main(args: Array[String]): Unit = {
        if (args.length != 1) {
            LOG.error("Usage: SparkSQLAnalyticsJob <config-file>")
            System.exit(1)
        }
        try {
            initSpark(args(0))
            sourceMgr = new DataSourceManager(spark)
            sinkMgr = new MultiSinkManager(spark)
            registerUDFs()
            startIngestion()
            startProcessing()
            startAggregation()
            awaitTermination()
        } catch {
            case e: Exception =>
                LOG.error("Job failed", e)
                cleanup()
                throw e
        }
    }

    private def initSpark(confPath: String): Unit = {
        val cfg = new SparkConfigManager(confPath)
        spark = SparkSession.builder()
          .appName("SparkSQL Analytics Pipeline")
          .config("spark.sql.adaptive.enabled", "true")
          .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
          .config("spark.sql.adaptive.skewJoin.enabled", "true")
          .config("spark.sql.streaming.checkpointLocation", cfg.checkpointLocation)
          .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
          .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .config("spark.sql.parquet.compression.codec", "snappy")
          .config("spark.sql.shuffle.partitions", "200")
          .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        LOG.info("Spark session initialized")
    }

    private def registerUDFs(): Unit = {
        spark.udf.register("cleanPhoneNumber", (s: String) =>
            Option(s).map(_.replaceAll("[^0-9]", "")).orNull
        )
        spark.udf.register("calculateAge", (bd: String) =>
            try {
                val birth = java.time.LocalDate.parse(bd)
                java.time.Period.between(birth, java.time.LocalDate.now()).getYears
            } catch {
                case _: Throwable => null
            }
        )
        spark.udf.register("getRegionFromIP", new GeoLocationUDF(), spark.sqlContext.sqlFunction("getRegionFromIP").dataType)
        LOG.info("UDFs registered")
    }

    private def startIngestion(): Unit = {
        val kafkaStream: Dataset[Row] = sourceMgr.createKafkaSource("event_topic", "earliest")
        val cleaned = new DataQualityProcessor(spark).process(kafkaStream)
        val bronzeQ = cleaned.writeStream
          .format("delta")
          .outputMode("append")
          .option("checkpointLocation", "/bronze/events/_checkpoint")
          .option("path", "/bronze/events")
          .trigger(Trigger.ProcessingTime("30 seconds"))
          .start()
        queries += "bronze" -> bronzeQ
        LOG.info("Ingestion stream started")
    }

    private def startProcessing(): Unit = {
        spark.sql(
            """
              |CREATE OR REPLACE TEMP VIEW bronze_events AS
              | SELECT * FROM delta.`/bronze/events`
              | WHERE event_time >= current_timestamp() - INTERVAL 1 HOUR
          """.stripMargin)
        val silver = spark.sql(
            """
              |WITH enriched AS (
              |  SELECT e.*, u.user_segment, p.category AS product_category,
              |         cleanPhoneNumber(u.phone) AS clean_phone,
              |         calculateAge(u.birth_date) AS user_age,
              |         getRegionFromIP(e.ip_address) AS user_region
              |  FROM bronze_events e
              |  LEFT JOIN user_profiles u ON e.user_id = u.user_id
              |  LEFT JOIN product_catalog p ON e.product_id = p.product_id
              |)
              |SELECT *,
              |  ROW_NUMBER() OVER (PARTITION BY user_id, session_id ORDER BY event_time) AS seq,
              |  LAG(event_time,1) OVER (PARTITION BY user_id, session_id ORDER BY event_time) AS prev_time,
              |  current_timestamp() AS processing_time
              |FROM enriched
          """.stripMargin)
        val silverQ = silver.writeStream
          .format("delta")
          .outputMode("append")
          .option("checkpointLocation", "/silver/events/_checkpoint")
          .option("path", "/silver/events")
          .partitionBy("date", "user_segment")
          .trigger(Trigger.ProcessingTime("1 minute"))
          .start()
        queries += "silver" -> silverQ
        LOG.info("Processing stream started")
    }

    private def startAggregation(): Unit = {
        spark.sql(
            """
              |CREATE OR REPLACE TEMP VIEW silver_events AS
              | SELECT * FROM delta.`/silver/events`
          """.stripMargin)

        val userMetrics = spark.sql(
            """
              |SELECT user_id, user_region,
              |       window(event_time, "5 minutes") AS time_win,
              |       COUNT(*) AS event_count,
              |       SUM(CASE WHEN event_type="purchase" THEN amount ELSE 0 END) AS total_revenue,
              |       current_timestamp() AS agg_time
              |FROM silver_events
              |WHERE event_time >= current_timestamp() - INTERVAL 1 HOUR
              |GROUP BY user_id, user_region, window(event_time,"5 minutes")
          """.stripMargin)

        val userQ = userMetrics.writeStream
          .foreachBatch { (batchDF: Dataset[Row], id: Long) =>
              sinkMgr.writeToMultipleTargets(batchDF, "user_metrics", id)
          }
          .outputMode("update")
          .trigger(Trigger.ProcessingTime("1 minute"))
          .start()

        queries += "userMetrics" -> userQ
        LOG.info("Aggregation stream started")
    }

    private def awaitTermination(): Unit = {
        sys.addShutdownHook(cleanup())
        queries.values.foreach(_.awaitTermination())
    }

    private def cleanup(): Unit = {
        LOG.info("Cleanup start")
        queries.foreach { case (name, q) =>
            if (q.isActive) {
                LOG.info(s"Stopping $name")
                q.stop()
            }
        }
        if (spark != null) spark.stop()
        LOG.info("Cleanup done")
    }
}
