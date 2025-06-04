# ===== DEPLOYMENT AND EXECUTION SCRIPTS =====

# 1. application.properties - Configuration File
cat > application.properties << 'EOF'
# Spark Configuration
spark.app.name=DataWarehouse-Pipeline
spark.executor.memory=4g
spark.executor.cores=2
spark.executor.instances=10
spark.driver.memory=2g
spark.driver.maxResultSize=2g
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
spark.sql.adaptive.skewJoin.enabled=true
spark.serializer=org.apache.spark.serializer.KryoSerializer
spark.sql.parquet.compression.codec=snappy

# Hive Configuration
hive.exec.dynamic.partition=true
hive.exec.dynamic.partition.mode=nonstrict
hive.exec.max.dynamic.partitions=10000
hive.exec.max.dynamic.partitions.pernode=1000

# HDFS Configuration
hdfs.replication.factor=3
hdfs.block.size=134217728

# Database Configuration
jdbc.url=jdbc:mysql://localhost:3306/source_db
jdbc.username=user
jdbc.password=password
jdbc.driver=com.mysql.cj.jdbc.Driver

# Resource Limits
max.concurrent.jobs=5
job.timeout.minutes=120
EOF

# 2. build.sbt - SBT Build Configuration
cat > build.sbt << 'EOF'
name := "DataWarehouse-Pipeline"
version := "1.0.0"
scalaVersion := "2.12.15"

val sparkVersion = "3.3.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "mysql" % "mysql-connector-java" % "8.0.33",
  "com.typesafe" % "config" % "1.4.2",
  "org.scalatest" %% "scalatest" % "3.2.15" % Test
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

assemblyJarName in assembly := "datawarehouse-pipeline-1.0.0.jar"
EOF

# 3. submit_job.sh - Job Submission Script
cat > submit_job.sh << 'EOF'
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

chmod +x submit_job.sh

# 4. setup_environment.sh - Environment Setup Script
cat > setup_environment.sh << 'EOF'
#!/bin/bash

# Data Warehouse Environment Setup Script

set -e

echo "Setting up Data Warehouse environment..."

# Create necessary directories
echo "Creating HDFS directories..."
hdfs dfs -mkdir -p /warehouse/raw
hdfs dfs -mkdir -p /warehouse/ods
hdfs dfs -mkdir -p /warehouse/dwd
hdfs dfs -mkdir -p /warehouse/dws
hdfs dfs -mkdir -p /warehouse/dm
hdfs dfs -mkdir -p /tmp/spark-logs

# Set permissions
echo "Setting HDFS permissions..."
hdfs dfs -chmod 755 /warehouse
hdfs dfs -chmod 755 /warehouse/*
hdfs dfs -chmod 777 /tmp/spark-logs

# Create Hive databases (will be done by application)
echo "Hive databases will be created by the application..."

# Setup logging directory
mkdir -p logs
mkdir -p target/scala-2.12

echo "Environment setup completed!"
echo "Next steps:"
echo "1. Build the project: sbt assembly"
echo "2. Submit a job: ./submit_job.sh [job_name] [partition]"
EOF

chmod +x setup_environment.sh

# 5. monitor_jobs.sh - Job Monitoring Script
cat > monitor_jobs.sh << 'EOF'
#!/bin/bash

# Data Warehouse Job Monitoring Script

echo "=========================================="
echo "Data Warehouse Job Monitor"
echo "=========================================="

# Check Spark applications
echo "Active Spark Applications:"
yarn application -list -appStates RUNNING | grep -i datawarehouse || echo "No active DWH jobs"

echo ""
echo "Recent Spark Applications:"
yarn application -list -appStates FINISHED,FAILED | head -10

echo ""
echo "HDFS Usage:"
hdfs dfs -du -h /warehouse

echo ""
echo "Hive Table Status:"
hive -e "SHOW DATABASES;" 2>/dev/null || echo "Hive not accessible"

echo ""
echo "System Resources:"
free -h
echo "CPU Usage:"
top -bn1 | grep "Cpu(s)" | awk '{print $2 $3 $4 $5}'

echo ""
echo "Recent Log Files:"
ls -lt logs/ | head -5
EOF

chmod +x monitor_jobs.sh

# 6. data_quality_check.sh - Data Quality Validation Script
cat > data_quality_check.sh << 'EOF'
#!/bin/bash

# Data Quality Check Script

PARTITION=${1:-$(date +%Y-%m-%d)}

echo "=========================================="
echo "Data Quality Check for partition: $PARTITION"
echo "=========================================="

# Check ODS layer
echo "Checking ODS layer..."
hive -e "
SELECT 'ods_customer' as table_name, count(*) as row_count
FROM ods_db.ods_customer WHERE dt='$PARTITION'
UNION ALL
SELECT 'ods_order' as table_name, count(*) as row_count
FROM ods_db.ods_order WHERE dt='$PARTITION';
"

# Check DWD layer
echo "Checking DWD layer..."
hive -e "
SELECT 'dwd_dim_customer' as table_name, count(*) as row_count
FROM dwd_db.dwd_dim_customer WHERE dt='$PARTITION'
UNION ALL
SELECT 'dwd_fact_order' as table_name, count(*) as row_count
FROM dwd_db.dwd_fact_order WHERE dt='$PARTITION';
"

# Check DWS layer
echo "Checking DWS layer..."
hive -e "
SELECT 'dws_customer_summary' as table_name, count(*) as row_count
FROM dws_db.dws_customer_summary WHERE dt='$PARTITION';
"

# Check for data quality issues
echo "Data Quality Checks:"
hive -e "
-- Check for null primary keys
SELECT 'NULL_CUSTOMER_IDS' as issue, count(*) as count
FROM ods_db.ods_customer
WHERE dt='$PARTITION' AND customer_id IS NULL

UNION ALL

-- Check for duplicate records
SELECT 'DUPLICATE_CUSTOMERS' as issue, count(*) as count
FROM (
  SELECT customer_id, count(*) as cnt
  FROM ods_db.ods_customer
  WHERE dt='$PARTITION'
  GROUP BY customer_id
  HAVING count(*) > 1
) t;
"

echo "Data quality check completed!"
EOF

chmod +x data_quality_check.sh

# 7. cleanup.sh - Cleanup Script
cat > cleanup.sh << 'EOF'
#!/bin/bash

# Cleanup old data and logs

DAYS_TO_KEEP=${1:-7}

echo "Cleaning up data older than $DAYS_TO_KEEP days..."

# Clean up old log files
echo "Cleaning log files..."
find logs/ -name "*.log" -type f -mtime +$DAYS_TO_KEEP -delete

# Clean up old Spark event logs
echo "Cleaning Spark event logs..."
hdfs dfs -rm -r -skipTrash /tmp/spark-logs/* 2>/dev/null || true

# Clean up old partitions (be careful with this in production)
echo "Note: Manual partition cleanup required for warehouse data"
echo "Consider implementing automatic partition cleanup based on retention policy"

# Clean up temporary files
echo "Cleaning temporary files..."
rm -rf /tmp/spark-*
rm -rf /tmp/hive-*

echo "Cleanup completed!"
EOF

chmod +x cleanup.sh

# 8. airflow_dag.py - Airflow DAG for Orchestration
cat > airflow_dag.py << 'EOF'
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.filesystem import FileSensor

# Default arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=15),
    'catchup': False
}

# Create DAG
dag = DAG(
    'datawarehouse_pipeline',
    default_args=default_args,
    description='Data Warehouse ETL Pipeline',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    max_active_runs=1,
    tags=['datawarehouse', 'etl']
)

# Task 1: Check source data availability
check_source_data = FileSensor(
    task_id='check_source_data',
    filepath='/raw/data/{{ ds }}',
    poke_interval=300,  # Check every 5 minutes
    timeout=3600,  # Timeout after 1 hour
    dag=dag
)

# Task 2: Setup environment
setup_env = BashOperator(
    task_id='setup_environment',
    bash_command='./setup_environment.sh',
    dag=dag
)

# Task 3: Raw to ODS
raw_to_ods = BashOperator(
    task_id='raw_to_ods',
    bash_command='./submit_job.sh raw-to-ods {{ ds }}',
    dag=dag
)

# Task 4: ODS to DWD
ods_to_dwd = BashOperator(
    task_id='ods_to_dwd',
    bash_command='./submit_job.sh ods-to-dwd {{ ds }}',
    dag=dag
)

# Task 5: DWD to DWS
dwd_to_dws = BashOperator(
    task_id='dwd_to_dws',
    bash_command='./submit_job.sh dwd-to-dws {{ ds }}',
    dag=dag
)

# Task 6: Data Quality Check
data_quality = BashOperator(
    task_id='data_quality_check',
    bash_command='./data_quality_check.sh {{ ds }}',
    dag=dag
)

# Task 7: Cleanup
cleanup = BashOperator(
    task_id='cleanup',
    bash_command='./cleanup.sh 7',
    dag=dag
)

# Define task dependencies
check_source_data >> setup_env >> raw_to_ods >> ods_to_dwd >> dwd_to_dws >> data_quality >> cleanup
EOF

# 9. docker-compose.yml - Local Development Environment
cat > docker-compose.yml << 'EOF'
version: '3.8'

services:
  namenode:
    image: bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
    container_name: namenode
    ports:
      - "9870:9870"
    environment:
      - CLUSTER_NAME=test
    env_file:
      - ./hadoop.env

  datanode:
    image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
    container_name: datanode
    depends_on:
      - namenode
    env_file:
      - ./hadoop.env

  spark-master:
    image: bde2020/spark-master:3.3.0-hadoop3.3
    container_name: spark-master
    ports:
      - "8080:8080"
      - "7077:7077"
    environment:
      - INIT_DAEMON_STEP=setup_spark

  spark-worker:
    image: bde2020/spark-worker:3.3.0-hadoop3.3
    container_name: spark-worker
    depends_on:
      - spark-master
    environment:
      - SPARK_MASTER=spark://spark-master:7077
    ports:
      - "8081:8081"

  hive-server:
    image: bde2020/hive:2.3.2-postgresql-metastore
    container_name: hive-server
    depends_on:
      - namenode
      - datanode
    env_file:
      - ./hadoop.env
    environment:
      - HIVE_CORE_CONF_javax_jdo_option_ConnectionURL=jdbc:postgresql://hive-metastore/metastore
    ports:
      - "10000:10000"

  hive-metastore:
    image: bde2020/hive:2.3.2-postgresql-metastore
    container_name: hive-metastore
    env_file:
      - ./hadoop.env
    command: /opt/hive/bin/hive --service metastore

  mysql:
    image: mysql:8.0
    container_name: mysql-source
    environment:
      MYSQL_ROOT_PASSWORD: rootpassword
      MYSQL_DATABASE: source_db
      MYSQL_USER: user
      MYSQL_PASSWORD: password
    ports:
      - "3306:3306"
    volumes:
      - mysql_data:/var/lib/mysql

volumes:
  mysql_data:
EOF

# 10. hadoop.env - Hadoop Environment Variables
cat > hadoop.env << 'EOF'
CORE_CONF_fs_defaultFS=hdfs://namenode:9000
CORE_CONF_hadoop_http_staticuser_user=root
CORE_CONF_hadoop_proxyuser_hue_hosts=*
CORE_CONF_hadoop_proxyuser_hue_groups=*
CORE_CONF_io_compression_codecs=org.apache.hadoop.io.compress.SnappyCodec

HDFS_CONF_dfs_webhdfs_enabled=true
HDFS_CONF_dfs_permissions_enabled=false
HDFS_CONF_dfs_namenode_datanode_registration_ip___hostname___check=false

YARN_CONF_yarn_log___aggregation___enable=true
YARN_CONF_yarn_log_server_url=http://historyserver:8188/applicationhistory/logs/
YARN_CONF_yarn_resourcemanager_recovery_enabled=true
YARN_CONF_yarn_resourcemanager_store_class=org.apache.hadoop.yarn.server.resourcemanager.recovery.FileSystemRMStateStore
YARN_CONF_yarn_resourcemanager_hostname=resourcemanager
YARN_CONF_yarn_resourcemanager_address=resourcemanager:8032
YARN_CONF_yarn_resourcemanager_scheduler_address=resourcemanager:8030
YARN_CONF_yarn_resourcemanager_resource___tracker_address=resourcemanager:8031
EOF

# 11. production_deploy.sh - Production Deployment Script
cat > production_deploy.sh << 'EOF'
#!/bin/bash

# Production Deployment Script

set -e

ENVIRONMENT=${1:-"production"}
VERSION=${2:-"1.0.0"}

echo "=========================================="
echo "Deploying Data Warehouse to $ENVIRONMENT"
echo "Version: $VERSION"
echo "=========================================="

# Build the project
echo "Building project..."
sbt clean assembly

# Create deployment package
echo "Creating deployment package..."
mkdir -p deploy
cp target/scala-2.12/datawarehouse-pipeline-*.jar deploy/
cp *.sh deploy/
cp *.properties deploy/
cp airflow_dag.py deploy/

# Create tarball
tar -czf datawarehouse-$VERSION.tar.gz deploy/

echo "Deployment package created: datawarehouse-$VERSION.tar.gz"

# Production deployment steps (customize for your environment)
if [ "$ENVIRONMENT" = "production" ]; then
    echo "Production deployment steps:"
    echo "1. Upload package to production servers"
    echo "2. Stop existing services"
    echo "3. Deploy new version"
    echo "4. Start services"
    echo "5. Run smoke tests"

    # Example deployment commands (customize for your infrastructure)
    # scp datawarehouse-$VERSION.tar.gz user@prod-server:/opt/datawarehouse/
    # ssh user@prod-server "cd /opt/datawarehouse && tar -xzf datawarehouse-$VERSION.tar.gz"
    # ssh user@prod-server "cd /opt/datawarehouse && ./setup_environment.sh"
fi

echo "Deployment completed!"
EOF

chmod +x production_deploy.sh

# 12. README.md - Documentation
cat > README.md << 'EOF'
# Data Warehouse Resource Management Framework

Complete end-to-end data warehouse solution with resource management, from raw data import to task submission.

## Architecture

```
Raw Data → ODS → DWD → DWS → Data Marts
```

- **Raw**: Source data from various systems
- **ODS**: Operational Data Store (staging area)
- **DWD**: Data Warehouse Detail (cleansed, integrated)
- **DWS**: Data Warehouse Summary (aggregated)

## Quick Start

1. **Setup Environment**
   ```bash
   ./setup_environment.sh
   ```

2. **Build Project**
   ```bash
   sbt assembly
   ```

3. **Submit Job**
   ```bash
   ./submit_job.sh full-pipeline 2024-01-01
   ```

## Available Jobs

- `raw-to-ods`: Import raw data to ODS layer
- `ods-to-dwd`: Process ODS to DWD layer
- `dwd-to-dws`: Aggregate DWD to DWS layer
- `full-pipeline`: Complete end-to-end pipeline

## Monitoring

```bash
./monitor_jobs.sh              # Check job status
./data_quality_check.sh        # Validate data quality
```

## Configuration

Edit `application.properties` for:
- Spark/Hive settings
- Resource allocation
- Database connections
- Performance tuning

## Production Deployment

```bash
./production_deploy.sh production 1.0.0
```

## Airflow Integration

Copy `airflow_dag.py` to your Airflow DAGs folder for automated scheduling.

## Docker Development

```bash
docker-compose up -d
```

Access:
- Hadoop NameNode: http://localhost:9870
- Spark Master: http://localhost:8080
- Hive Server: localhost:10000

## Maintenance

```bash
./cleanup.sh 7    # Clean up data older than 7 days
```
EOF

echo "=========================================="
echo "Complete Data Warehouse Framework Generated"
echo "=========================================="
echo "Files created:"
echo "- Resource management code (Scala)"
echo "- Configuration files"
echo "- Deployment scripts"
echo "- Monitoring tools"
echo "- Docker setup"
echo "- Airflow DAG"
echo "- Production deployment"
echo "=========================================="