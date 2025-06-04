# Ensure HADOOP_CONF_DIR and YARN_CONF_DIR are set in your environment if not managed by the cluster
# export HADOOP_CONF_DIR=/path/to/hadoop/conf
# export YARN_CONF_DIR=/path/to/yarn/conf

# Variables
APP_JAR="target/my-spark-dw-project-1.0.0-jar-with-dependencies.jar"
MAIN_CLASS="com.yourcompany.dw.MainApp"
JOB_TYPE="all" # or "ods-dwd", "dwd-dws"
LOAD_DATE="2025-05-30" # Or dynamically generate this

# Example spark-submit command
spark-submit \
  --master yarn \
  --deploy-mode cluster \ # 'client' mode for debugging, 'cluster' for production
  --name "SparkDW_ETL_${JOB_TYPE}_${LOAD_DATE}" \
  --class ${MAIN_CLASS} \
  --driver-memory 1g \
  --executor-memory 2g \
  --num-executors 5 \
  --executor-cores 2 \
  --queue your_yarn_queue \ # Specify your YARN queue
  --files src/main/resources/log4j2.properties#log4j2.properties,src/main/resources/application.conf#application.conf \ # Bundle config files
  --conf "spark.yarn.maxAppAttempts=2" \
  --conf "spark.yarn.submit.waitAppCompletion=true" \ # or false if you want to submit and not wait
  --conf "spark.eventLog.enabled=true" \ # For Spark History Server
  --conf "spark.eventLog.dir=hdfs:///spark-logs" \ # Configure your Spark event log directory
  # Add any other Spark configurations needed, e.g., for Kryo serializer, shuffle service, etc.
  # --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
  # --conf "spark.sql.adaptive.enabled=true" \ # For Spark 3.x
  ${APP_JAR} \
  ${JOB_TYPE} \
  ${LOAD_DATE}