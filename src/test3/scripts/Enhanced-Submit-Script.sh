#!/bin/bash

set -e  # Exit on any error

SPARK_HOME="/opt/spark"
JAR_PATH="target/spark-hive-dwh-1.0.0.jar"
MAIN_CLASS="com.company.dwh.SparkHiveDWHApp"
BATCH_DATE=${1:-$(date +%Y-%m-%d)}
LOG_FILE="logs/spark-dwh-${BATCH_DATE}.log"

# Create logs directory
mkdir -p logs

echo "Starting Spark DWH pipeline for batch date: $BATCH_DATE"

# Submit Spark job with comprehensive configuration
$SPARK_HOME/bin/spark-submit \
  --class $MAIN_CLASS \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 2g \
  --executor-memory 4g \
  --executor-cores 2 \
  --num-executors 4 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --conf spark.sql.adaptive.skewJoin.enabled=true \
  --conf spark.sql.warehouse.dir=/user/hive/warehouse \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.sql.execution.arrow.pyspark.enabled=true \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes=256MB \
  --conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128MB \
  --files src/main/resources/application.conf \
  $JAR_PATH $BATCH_DATE 2>&1 | tee $LOG_FILE

EXIT_CODE=${PIPESTATUS[0]}

if [ $EXIT_CODE -eq 0 ]; then
    echo "Pipeline completed successfully for $BATCH_DATE"
    # Optional: Send success notification
else
    echo "Pipeline failed for $BATCH_DATE with exit code $EXIT_CODE"
    # Optional: Send failure alert
    exit $EXIT_CODE
fi