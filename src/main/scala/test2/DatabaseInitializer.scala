package test2

import org.apache.spark.sql._

import test2.config.DatabaseConfig

// ===== DATABASE INITIALIZER =====
class DatabaseInitializer(spark: SparkSession, resourceManager: ResourceManager) {

    def initializeDataWarehouse(): Unit = {
        val databases = Seq(
            DatabaseConfig("raw_db", "/warehouse/raw", "Raw data layer"),
            DatabaseConfig("ods_db", "/warehouse/ods", "Operational Data Store"),
            DatabaseConfig("dwd_db", "/warehouse/dwd", "Data Warehouse Detail"),
            DatabaseConfig("dws_db", "/warehouse/dws", "Data Warehouse Summary"),
            DatabaseConfig("dm_db", "/warehouse/dm", "Data Mart")
        )

        databases.foreach(createDatabase)
    }

    private def createDatabase(config: DatabaseConfig): Unit = {
        try {
            // Create HDFS directories
            resourceManager.createHDFSDirectory(config.location)

            // Create Hive database
            spark.sql(s"""
                CREATE DATABASE IF NOT EXISTS ${config.name}
                COMMENT '${config.description}'
                LOCATION '${config.location}'
              """)

            println(s"Database ${config.name} initialized successfully")

        } catch {
            case ex: Exception =>
                println(s"Failed to create database ${config.name}: ${ex.getMessage}")
                throw ex
        }
    }

    def createODSTables(): Unit = {
        // Customer ODS Table
        spark.sql("""
              CREATE TABLE IF NOT EXISTS ods_db.ods_customer (
                customer_id BIGINT,
                customer_code STRING,
                customer_name STRING,
                customer_type STRING,
                registration_date DATE,
                status STRING,
                phone STRING,
                email STRING,
                address STRING,
                create_time TIMESTAMP,
                update_time TIMESTAMP
              )
              PARTITIONED BY (dt STRING)
              STORED AS PARQUET
              LOCATION '/warehouse/ods/ods_customer'
            """)

        // Order ODS Table
        spark.sql("""
              CREATE TABLE IF NOT EXISTS ods_db.ods_order (
                order_id BIGINT,
                order_no STRING,
                customer_id BIGINT,
                order_date DATE,
                order_amount DECIMAL(15,2),
                currency STRING,
                order_status STRING,
                payment_method STRING,
                create_time TIMESTAMP,
                update_time TIMESTAMP
              )
              PARTITIONED BY (dt STRING)
              STORED AS PARQUET
              LOCATION '/warehouse/ods/ods_order'
            """)

        println("ODS tables created successfully")
    }

    def createDWDTables(): Unit = {
        // Customer Dimension
        spark.sql("""
              CREATE TABLE IF NOT EXISTS dwd_db.dwd_dim_customer (
                customer_sk BIGINT,
                customer_id BIGINT,
                customer_code STRING,
                customer_name STRING,
                customer_type STRING,
                registration_date DATE,
                status STRING,
                phone STRING,
                email STRING,
                address STRING,
                effective_date DATE,
                expiry_date DATE,
                is_current INT,
                data_source STRING,
                etl_create_time TIMESTAMP,
                etl_update_time TIMESTAMP,
                etl_batch_id STRING
              )
              PARTITIONED BY (dt STRING)
              STORED AS PARQUET
              LOCATION '/warehouse/dwd/dwd_dim_customer'
            """)

        // Order Fact
        spark.sql("""
              CREATE TABLE IF NOT EXISTS dwd_db.dwd_fact_order (
                order_sk BIGINT,
                order_id BIGINT,
                order_no STRING,
                customer_sk BIGINT,
                customer_id BIGINT,
                order_date DATE,
                order_amount DECIMAL(15,2),
                order_amount_usd DECIMAL(15,2),
                currency STRING,
                order_status STRING,
                payment_method STRING,
                etl_create_time TIMESTAMP,
                etl_update_time TIMESTAMP,
                etl_batch_id STRING
              )
              PARTITIONED BY (dt STRING)
              STORED AS PARQUET
              LOCATION '/warehouse/dwd/dwd_fact_order'
            """)

        println("DWD tables created successfully")
    }

    def createDWSTables(): Unit = {
        // Customer Summary
        spark.sql("""
              CREATE TABLE IF NOT EXISTS dws_db.dws_customer_summary (
                customer_id BIGINT,
                customer_name STRING,
                customer_type STRING,
                total_orders BIGINT,
                total_amount_usd DECIMAL(15,2),
                avg_order_amount DECIMAL(15,2),
                last_order_date DATE,
                first_order_date DATE,
                stat_date STRING,
                customer_level STRING,
                etl_create_time TIMESTAMP,
                etl_update_time TIMESTAMP,
                etl_batch_id STRING
              )
              PARTITIONED BY (dt STRING)
              STORED AS PARQUET
              LOCATION '/warehouse/dws/dws_customer_summary'
            """)

        println("DWS tables created successfully")
    }
}
