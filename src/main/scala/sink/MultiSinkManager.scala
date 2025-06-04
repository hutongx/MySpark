package sink

import com.sun.org.slf4j.internal.LoggerFactory
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

class MultiSinkManager(spark: SparkSession) {
    private val LOG = LoggerFactory.getLogger(getClass)
    private val jedisPool = new JedisPool("localhost")

    def writeToMultipleTargets(df: DataFrame, dataType: String, batchId: Long): Unit = {
        writeDelta(df, dataType)
        writeMySQL(df, dataType)
        writeES(df, dataType)
        writeRedis(df, dataType)
    }

    private def writeDelta(df: DataFrame, dt: String): Unit = {
        df.write.format("delta").mode(SaveMode.Append)
          .option("mergeSchema", "true")
          .save(s"/gold/$dt")
        LOG.info(s"Written to Delta: /gold/$dt")
    }

    private def writeMySQL(df: DataFrame, table: String): Unit = {
        if (table.endsWith("metrics")) {
            df.write.format("jdbc")
              .option("url", "jdbc:mysql://localhost:3306/analytics")
              .option("dbtable", table)
              .option("user", "analytics_user")
              .option("password", "password")
              .mode(SaveMode.Append)
              .save()
            LOG.info(s"Written to MySQL: $table")
        }
    }

    private def writeES(df: DataFrame, dt: String): Unit = {
        df.sample(0.1).write.format("org.elasticsearch.spark.sql")
          .option("es.resource", s"analytics_$dt/doc")
          .option("es.nodes", "localhost")
          .option("es.port", "9200")
          .mode(SaveMode.Append)
          .save()
        LOG.info("Written to Elasticsearch")
    }

    private def writeRedis(df: DataFrame, dt: String): Unit = {
        if (dt == "user_metrics") {
            df.filter("total_revenue > 1000").foreachPartition { iter =>
                val jedis = jedisPool.getResource
                iter.foreach { row =>
                    val user = row.getAs[String]("user_id")
                    val rev  = row.getAs[Double]("total_revenue")
                    jedis.hset("hot_users", user, rev.toString)
                }
                jedis.close()
            }
            LOG.info("Cached to Redis")
        }
    }
}
