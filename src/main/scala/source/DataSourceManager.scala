package source

class DataSourceManager(spark: SparkSession) {
    private val schema = StructType(Seq(
        StructField("event_id", StringType),
        StructField("user_id", StringType),
        StructField("session_id", StringType),
        StructField("event_type", StringType),
        StructField("event_time", TimestampType),
        StructField("product_id", StringType),
        StructField("amount", DoubleType),
        StructField("ip_address", StringType),
        StructField("user_agent", StringType),
        StructField("properties", MapType(StringType,StringType))
    ))

    def createKafkaSource(topic: String, offsets: String): Dataset[Row] = {
        val df = spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "localhost:9092")
          .option("subscribe", topic)
          .option("startingOffsets", offsets)
          .option("failOnDataLoss", "false")
          .load()

        df.selectExpr("CAST(value AS STRING) AS json_str", "timestamp")
          .select(from_json(col("json_str"), schema).as("data"), col("timestamp"))
          .select("data.*", "timestamp")
    }

    def createJDBCSource(table: String, url: String, user: String, pwd: String): DataFrame = {
        spark.read.format("jdbc")
          .option("url", url)
          .option("dbtable", table)
          .option("user", user)
          .option("password", pwd)
          .option("driver", "com.mysql.cj.jdbc.Driver")
          .load()
    }

    def createDeltaSource(path: String): Dataset[Row] =
        spark.readStream.format("delta").load(path)
}
