# ====================================
# deploy.sh - 部署脚本
# ====================================
#!/bin/bash

set -e

# 配置变量
PROJECT_NAME="datawarehouse-etl"
VERSION="1.0.0"
JAR_NAME="${PROJECT_NAME}-${VERSION}.jar"
MAIN_CLASS="com.datawarehouse.DataWarehouseApplication"
SCHEDULER_CLASS="com.datawarehouse.scheduler.SchedulerMain"

# Hadoop和Spark配置
HADOOP_HOME="/opt/hadoop"
SPARK_HOME="/opt/spark"
YARN_CONF_DIR="/opt/hadoop/etc/hadoop"

# 项目目录
PROJECT_DIR="/opt/datawarehouse"
LOG_DIR="/var/log/datawarehouse"
PID_DIR="/var/run/datawarehouse"

echo "======================================="
echo "开始部署数据仓库ETL项目"
echo "======================================="

# 1. 创建必要目录
create_directories() {
    echo "创建项目目录..."
    sudo mkdir -p $PROJECT_DIR/{bin,lib,conf,logs}
    sudo mkdir -p $LOG_DIR
    sudo mkdir -p $PID_DIR
    sudo chown -R hadoop:hadoop $PROJECT_DIR $LOG_DIR $PID_DIR
}

# 2. 构建项目
build_project() {
    echo "构建Maven项目..."
    mvn clean package -DskipTests

    if [ ! -f "target/$JAR_NAME" ]; then
        echo "错误: JAR文件构建失败"
        exit 1
    fi
}

# 3. 部署文件
deploy_files() {
    echo "部署项目文件..."

    # 复制JAR文件
    cp target/$JAR_NAME $PROJECT_DIR/lib/

    # 复制配置文件
    cp src/main/resources/application.conf $PROJECT_DIR/conf/
    cp src/main/resources/log4j2.xml $PROJECT_DIR/conf/

    # 复制脚本文件
    cp scripts/*.sh $PROJECT_DIR/bin/
    chmod +x $PROJECT_DIR/bin/*.sh
}

# 4. 创建启动脚本
create_startup_scripts() {
    echo "创建启动脚本..."

    # ETL任务启动脚本
    cat > $PROJECT_DIR/bin/start-etl.sh << 'EOF'
#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

JAR_FILE="$PROJECT_DIR/lib/datawarehouse-etl-1.0.0.jar"
MAIN_CLASS="com.datawarehouse.DataWarehouseApplication"
CONF_FILE="$PROJECT_DIR/conf/application.conf"

# Spark配置
SPARK_SUBMIT_OPTIONS="
    --class $MAIN_CLASS
    --master yarn
    --deploy-mode cluster
    --driver-memory 2g
    --executor-memory 4g
    --executor-cores 2
    --num-executors 4
    --conf spark.sql.adaptive.enabled=true
    --conf spark.sql.adaptive.coalescePartitions.enabled=true
    --conf spark.sql.warehouse.dir=hdfs://namenode:9000/user/hive/warehouse
    --conf spark.hadoop.hive.metastore.uris=thrift://metastore:9083
    --files $CONF_FILE
"

# 执行Spark任务
$SPARK_HOME/bin/spark-submit $SPARK_SUBMIT_OPTIONS $JAR_FILE "$@"
EOF

    # 调度器启动脚本
    cat > $PROJECT_DIR/bin/start-scheduler.sh << 'EOF'
#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

JAR_FILE="$PROJECT_DIR/lib/datawarehouse-etl-1.0.0.jar"

MAIN_CLASS="com.datawarehouse.scheduler.SchedulerMain"
# Spark配置
SPARK_SUBMIT_OPTIONS="
    --class $MAIN_CLASS
    --master yarn
    --deploy-mode cluster
    --driver-memory 1g
    --executor-memory 2g
    --executor-cores 1
    --num-executors 2
    --conf spark.sql.adaptive.enabled=true
    --conf spark.sql.adaptive.coalescePartitions.enabled=true
    --conf spark.sql.warehouse.dir=hdfs://namenode:9000/user/hive/warehouse
    --conf spark.hadoop.hive.metastore.uris=thrift://metastore:9083
    --files $PROJECT_DIR/conf/application.conf
"
# 执行Spark任务
$SPARK_HOME/bin/spark-submit $SPARK_SUBMIT_OPTIONS $JAR_FILE "$@"
EOF

    # 设置脚本权限
    chmod +x $PROJECT_DIR/bin/start-etl.sh
    chmod +x $PROJECT_DIR/bin/start-scheduler.sh
}
# 5. 创建日志和PID目录
create_log_and_pid_dirs() {
    echo "创建日志和PID目录..."
    sudo mkdir -p $LOG_DIR
    sudo mkdir -p $PID_DIR
    sudo chown -R hadoop:hadoop $LOG_DIR $PID_DIR
}
# 6. 启动服务
start_services() {
    echo "启动数据仓库ETL服务..."

    # 启动ETL任务
    $PROJECT_DIR/bin/start-etl.sh > $LOG_DIR/etl.log 2>&1 &
    echo $! > $PID_DIR/etl.pid

    # 启动调度器
    $PROJECT_DIR/bin/start-scheduler.sh > $LOG_DIR/scheduler.log 2>&1 &
    echo $! > $PID_DIR/scheduler.pid

    echo "服务已启动，日志文件位于: $LOG_DIR"
}
# 执行部署步骤
create_directories
build_project
deploy_files
create_startup_scripts
create_log_and_pid_dirs
start_services
echo "======================================="
echo "数据仓库ETL项目部署完成"
echo "======================================="
# 检查Hadoop和Spark环境变量
if [ -z "$HADOOP_HOME" ] || [ -z "$SPARK_HOME" ]; then
    echo "错误: 请确保HADOOP_HOME和SPARK_HOME环境变量已设置"
    exit 1
fi
# 检查Maven是否安装
if ! command -v mvn &> /dev/null; then
    echo "错误: Maven未安装，请先安装Maven"
    exit 1
fi
# 检查Hadoop和Spark是否安装
if [ ! -d "$HADOOP_HOME" ]; then
    echo "错误: Hadoop未安装或路径不正确: $HADOOP_HOME"
    exit 1
fi
if [ ! -d "$SPARK_HOME" ]; then
    echo "错误: Spark未安装或路径不正确: $SPARK_HOME"
    exit 1
fi
# 检查日志目录是否可写
if [ ! -w "$LOG_DIR" ]; then
    echo "错误: 日志目录不可写: $LOG_DIR"
    exit 1
fi
# 检查PID目录是否可写
if [ ! -w "$PID_DIR" ]; then
    echo "错误: PID目录不可写: $PID_DIR"
    exit 1
fi
# 检查JAR文件是否存在
if [ ! -f "target/$JAR_NAME" ]; then
    echo "错误: JAR文件不存在，请先构建项目"
    exit 1
fi
# 检查配置文件是否存在
if [ ! -f "src/main/resources/application.conf" ]; then
    echo "错误: 配置文件不存在，请检查项目结构"
    exit 1
fi
# 检查脚本目录是否存在
if [ ! -d "scripts" ]; then
    echo "错误: 脚本目录不存在，请检查项目结构"
    exit 1
fi
# 检查脚本文件是否存在
if [ ! -f "scripts/start-etl.sh" ] || [ ! -f "scripts/start-scheduler.sh" ]; then
    echo "错误: 启动脚本文件不存在，请检查项目结构"
    exit 1
fi
# 检查脚本文件是否可执行
if [ ! -x "scripts/start-etl.sh" ] || [ ! -x "scripts/start-scheduler.sh" ]; then
    echo "错误: 启动脚本文件不可执行，请检查文件权限"
    exit 1
fi
# 检查日志文件是否可写
if [ ! -w "$LOG_DIR/etl.log" ] || [ ! -w "$LOG_DIR/scheduler.log" ]; then
    echo "错误: 日志文件不可写，请检查日志目录权限"
    exit 1
fi
# 检查PID文件是否可写
if [ ! -w "$PID_DIR/etl.pid" ] || [ ! -w "$PID_DIR/scheduler.pid" ]; then
    echo "错误: PID文件不可写，请检查PID目录权限"
    exit 1
fi
# 检查Hadoop和Spark配置文件是否存在
if [ ! -f "$YARN_CONF_DIR/yarn-site.xml" ]; then
    echo "错误: Hadoop YARN配置文件不存在，请检查Hadoop安装"
    exit 1
fi
if [ ! -f "$SPARK_HOME/conf/spark-defaults.conf" ]; then
    echo "错误: Spark默认配置文件不存在，请检查Spark安装"
    exit 1
fi
# 检查Hadoop和Spark版本
HADOOP_VERSION=$($HADOOP_HOME/bin/hadoop version | grep "Hadoop" | awk '{print $2}')
SPARK_VERSION=$($SPARK_HOME/bin/spark-submit --version | grep "version" | awk '{print $5}')
if [ -z "$HADOOP_VERSION" ]; then
    echo "错误: 无法获取Hadoop版本，请检查Hadoop安装"
    exit 1
fi
if [ -z "$SPARK_VERSION" ]; then
    echo "错误: 无法获取Spark版本，请检查Spark安装"
    exit 1
fi
echo "Hadoop版本: $HADOOP_VERSION"
echo "Spark版本: $SPARK_VERSION"
echo "======================================="
echo "部署脚本执行完成"
echo "======================================="