#!/bin/bash

# submit_job.sh - Production job submission script

# Set environment variables
export HADOOP_CONF_DIR=/etc/hadoop/conf
export HIVE_CONF_DIR=/etc/hive/conf
export SPARK_HOME=/opt/spark

# Job configuration
APP_NAME="HiveDataPipeline"
MAIN_CLASS="com.bigdata.pipeline.DataPipelineMain"
JAR_FILE="target/spark-hive-pipeline-1.0.0-jar-with-dependencies.jar"
PROCESS_DATE=${1:-$(date +%Y-%m-%d)}

# Spark submit configuration
SPARK_SUBMIT_ARGS="
--class $MAIN_CLASS
--master yarn
--deploy-mode cluster
--name $APP_NAME-$PROCESS_DATE
--conf spark.app.name=$APP_NAME
--conf spark.sql.adaptive.enabled=true
--conf spark.sql.adaptive.coalescePartitions.enabled=true
--conf spark.sql.adaptive.skewJoin.enabled=true
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer
--conf spark.sql.warehouse.dir=/user/hive/warehouse
--conf hive.metastore.uris=thrift://hive-metastore:9083
--conf hive.exec.dynamic.partition=true
--conf hive.exec.dynamic.partition.mode=nonstrict
--num-executors 10
--executor-cores 4
--executor-memory 8g
--driver-cores 2
--driver-memory 4g
--driver-java-options '-Dlog4j.configuration=file:log4j.properties'
--files log4j.properties,application.conf
--jars /opt/hive/lib/hive-exec-3.1.2.jar,/opt/hadoop/share/hadoop/common/hadoop-common-3.3.1.jar
$JAR_FILE
$PROCESS_DATE"

echo "=========================================="
echo "Starting Spark Job Submission"
echo "=========================================="
echo "Job Name: $APP_NAME"
echo "Process Date: $PROCESS_DATE"
echo "JAR File: $JAR_FILE"
echo "=========================================="

# Check if JAR file exists
if [ ! -f "$JAR_FILE" ]; then
    echo "ERROR: JAR file not found: $JAR_FILE"
    echo "Please build the project first using: mvn clean package"
    exit 1
fi

# Submit the job
echo "Submitting Spark job..."
$SPARK_HOME/bin/spark-submit $SPARK_SUBMIT_ARGS

# Check submission status
if [ $? -eq 0 ]; then
    echo "=========================================="
    echo "Job submitted successfully!"
    echo "=========================================="

    # Get application ID for monitoring
    APP_ID=$(yarn application -list -appStates RUNNING | grep $APP_NAME-$PROCESS_DATE | awk '{print $1}')
    if [ ! -z "$APP_ID" ]; then
        echo "Application ID: $APP_ID"
        echo "Monitor job at: http://yarn-resourcemanager:8088/proxy/$APP_ID/"
        echo "View logs with: yarn logs -applicationId $APP_ID"
    fi
else
    echo "=========================================="
    echo "Job submission failed!"
    echo "=========================================="
    exit 1
fi

# Optional: Wait for job completion and check status
read -p "Wait for job completion? (y/n): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Waiting for job completion..."

    while true; do
        STATUS=$(yarn application -status $APP_ID 2>/dev/null | grep "Final-State" | awk '{print $3}')

        if [ "$STATUS" = "SUCCEEDED" ]; then
            echo "Job completed successfully!"
            break
        elif [ "$STATUS" = "FAILED" ] || [ "$STATUS" = "KILLED" ]; then
            echo "Job failed with status: $STATUS"
            echo "Check logs with: yarn logs -applicationId $APP_ID"
            exit 1
        else
            echo "Job is still running... (Status: $STATUS)"
            sleep 30
        fi
    done
fi

echo "=========================================="
echo "Pipeline execution completed"
echo "=========================================="