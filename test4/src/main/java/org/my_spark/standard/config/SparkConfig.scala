package org.my_spark.standard.config

case class SparkConfig(
                        appName: String,
                        master: String,
                        deployMode: String,
                        sparkConf: Map[String, String]
                    )

