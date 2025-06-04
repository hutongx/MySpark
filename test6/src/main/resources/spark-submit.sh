# After packaging your application into a fat JAR (e.g., spark-dw-etl-pipeline-1.0.0-jar-with-dependencies.jar)
# using mvn clean package, you can submit it to YARN.

#!/bin/bash

# Ensure HADOOP_CONF_DIR and YARN_CONF_DIR are set in your environment,
# or that your spark-submit is configured to find them.

# --- Variables ---
# Path to your fat JAR
APP_JAR_NAME="spark-dw-etl-pipeline-1.0.0-jar-with-dependencies.jar" # Adjust version as needed
APP_JAR_PATH="./target/${APP_JAR_NAME}" # Relative to project root, or provide absolute path

MAIN_CLASS="com.yourcompany.dw.MainApp"

# Job parameters (these would typically come from your scheduler like Airflow)
JOB_NAME="full-etl" # e.g., "ods-dwd", "dwd-dws", "full-etl"
LOAD_DATE=$(date +%Y-%m-%d) # Today's date, or a specific date for backfills
# Optional: specific sources for the job, comma-separated. Leave empty for all.
# SOURCES_TO_PROCESS="users,orders"
SOURCES_TO_PROCESS=""
RUN_ALL_FLAG="--run-all" # Add this if you want the job to process all its defined steps/sources

# YARN and Spark configurations
DEPLOY_MODE="cluster" # 'client' for debugging, 'cluster' for production
DRIVER_MEMORY="2g"
EXECUTOR_MEMORY="4g"
NUM_EXECUTORS="5"
EXECUTOR_CORES="2"
YARN_QUEUE="default" # Specify your target YARN queue

# --- Build application arguments ---
APP_ARGS="--job-name ${JOB_NAME} --load-date ${LOAD_DATE}"
if [ -n "${SOURCES_TO_PROCESS}" ]; then
  APP_ARGS="${APP_ARGS} --sources ${SOURCES_TO_PROCESS}"
fi
if [ "${RUN_ALL_FLAG}" == "--run-all" ]; then
    APP_ARGS="${APP_ARGS} ${RUN_ALL_FLAG}"
fi


echo "Submitting Spark application to YARN..."
echo "JAR: ${APP_JAR_PATH}"
echo "Main Class: ${MAIN_CLASS}"
echo "App Args: ${APP_ARGS}"
echo "YARN Queue: ${YARN_QUEUE}"

spark-submit \
  --master yarn \
  --deploy-mode ${DEPLOY_MODE} \
  --name "SparkDW_ETL_${JOB_NAME}_${LOAD_DATE}" \
  --class ${MAIN_CLASS} \
  --driver-memory ${DRIVER_MEMORY} \
  --executor-memory ${EXECUTOR_MEMORY} \
  --num-executors ${NUM_EXECUTORS} \
  --executor-cores ${EXECUTOR_CORES} \
  --queue ${YARN_QUEUE} \
  --conf "spark.yarn.maxAppAttempts=2" \
  --conf "spark.yarn.submit.waitAppCompletion=false" \ # Set to true if your script needs to wait
  --conf "spark.eventLog.enabled=true" \
  --conf "spark.eventLog.dir=hdfs:///spark-logs" \ # Ensure this HDFS directory exists and is writable
  --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
  --conf "spark.sql.adaptive.enabled=true" \ # Recommended for Spark 3.x+
  --conf "spark.sql.adaptive.coalescePartitions.enabled=true" \
  --conf "spark.sql.sources.partitionOverwriteMode=dynamic" \ # Important for dynamic partition overwrites
  # --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \ # If using Delta Lake
  # --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" # If using Delta Lake
  # Add any other application-specific or cluster-specific Spark configurations here
  # Example: --conf "spark.driver.extraJavaOptions=-Dconfig.file=my_prod_app.conf" if using external config
  # Example: --files /path/to/your/external_application.conf#application.conf
  # Note: application.conf and log4j2.properties are bundled in the JAR if in src/main/resources

  ${APP_JAR_PATH} \
  ${APP_ARGS} # Pass arguments to your MainApp

EXIT_CODE=$?
if [ ${EXIT_CODE} -eq 0 ]; then
  echo "Spark application submitted successfully (or completed if waitAppCompletion=true)."
else
  echo "Spark application submission FAILED with exit code ${EXIT_CODE}."
  exit ${EXIT_CODE}
fi

echo "To monitor, check the YARN ResourceManager UI or use: yarn application -status <application_id>"