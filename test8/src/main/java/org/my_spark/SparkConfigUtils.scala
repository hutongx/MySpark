package org.my_spark

import com.typesafe.config.{Config, ConfigFactory}

/**
 * Spark配置工具类
 */
object SparkConfigUtils {

    private val config: Config = ConfigFactory.load()

    /**
     * 获取数据库名称
     */
    def getDatabaseName(layer: String): String = {
        config.getString(s"datawarehouse.databases.$layer")
    }

    /**
     * 获取HDFS基础路径
     */
    def getHdfsBasePath: String = {
        val hdfsConfig = config.getConfig("datawarehouse.datasource.hdfs")
        s"${hdfsConfig.getString("namenode")}${hdfsConfig.getString("base-path")}"
    }

    /**
     * 获取分区日期格式
     */
    def getPartitionDateFormat: String = {
        config.getString("datawarehouse.partition.date-format")
    }

    /**
     * 获取数据保留天数
     */
    def getRetentionDays: Int = {
        config.getInt("datawarehouse.partition.retention-days")
    }
}
