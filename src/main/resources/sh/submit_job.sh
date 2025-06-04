#!/bin/bash

# Data Warehouse Job Submission Script
# Usage: ./submit_job.sh [job_name] [partition] [config_file]

set -e

# Default parameters
JOB_NAME=${1:-"full-pipeline"}
PARTITION=${2:-$(date +%Y-%m-%d)}
CONFIG_FILE=${3:-"application.properties"}

# Spark and Hadoop paths
SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
HADOOP_HOME=${HADOOP_HOME:-"/opt/hadoop"}
HIVE_HOME=${HIVE_HOME:-"/opt/hive"}

# JAR file location
JAR_FILE="target/scala-2.12/datawarehouse-pipeline-1.0.0.jar"
MAIN_CLASS="DataWarehouseResourceManager"

# Logging
LOG_DIR="logs"
mkdir -p $LOG_DIR
LOG_FILE="$LOG_DIR/dwh_job_${JOB_NAME}_${PARTITION}_$(date +%Y%m%d_%H%M%S).log"

echo "=========================================="
echo "Data Warehouse Job Submission"
echo "=========================================="
echo "Job Name: $JOB_NAME"
echo "Partition: $PARTITION"
echo "Config File: $CONFIG_FILE"
echo "Log File: $LOG_FILE"
echo "=========================================="

# Check if JAR file exists
if [ ! -f "$JAR_FILE" ]; then
    echo "Error: JAR file not found at $JAR_FILE"
    echo "Please build the project first: sbt assembly"
    exit 1
fi

# Check if config file exists
if [ ! -f "$CONFIG_FILE" ]; then
    echo "Error: Configuration file not found at $CONFIG_FILE"
    exit 1
fi

# Load configuration
source $CONFIG_FILE

# Submit Spark job
$SPARK_HOME/bin/spark-submit \
    --class $MAIN_CLASS \
    --master yarn \
    --deploy-mode cluster \
    --executor-memory ${spark.executor.memory:-4g} \
    --executor-cores ${spark.executor.cores:-2} \
    --num-executors ${spark.executor.instances:-10} \
    --driver-memory ${spark.driver.memory:-2g} \
    --driver-cores 2 \
    --conf spark.app.name="DWH-${JOB_NAME}-${PARTITION}" \
    --conf spark.sql.adaptive.enabled=${spark.sql.adaptive.enabled:-true} \
    --conf spark.sql.adaptive.coalescePartitions.enabled=${spark.sql.adaptive.coalescePartitions.enabled:-true} \
    --conf spark.serializer=${spark.serializer:-org.apache.spark.serializer.KryoSerializer} \
    --conf spark.sql.parquet.compression.codec=${spark.sql.parquet.compression.codec:-snappy} \
    --conf spark.sql.hive.metastore.version=2.3.9 \
    --conf spark.sql.hive.metastore.jars=builtin \
    --files $CONFIG_FILE \
    --jars ${HIVE_HOME}/lib/mysql-connector-java-8.0.33.jar \
    $JAR_FILE \
    $JOB_NAME $PARTITION $CONFIG_FILE \
    2>&1 | tee $LOG_FILE

# Check job status
if [ ${PIPESTATUS[0]} -eq 0 ]; then
    echo "=========================================="
    echo "Job completed successfully!"
    echo "Log file: $LOG_FILE"
    echo "=========================================="
else
    echo "=========================================="
    echo "Job failed! Check log file: $LOG_FILE"
    echo "=========================================="
    exit 1
fi
EOF