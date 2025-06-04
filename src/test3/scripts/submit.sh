#!/bin/bash

SPARK_HOME="/opt/spark"
JAR_PATH="target/spark-hive-dwh-1.0.0.jar"
MAIN_CLASS="com.company.dwh.SparkHiveDWHApp"
BATCH_DATE=${1:-$(date +%Y-%m-%d)}

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
  $JAR_PATH $BATCH_DATE