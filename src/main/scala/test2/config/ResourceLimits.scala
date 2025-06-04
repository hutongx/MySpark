package test2.config

case class ResourceLimits(
   executorMemory: String = "4g",
   executorCores: Int = 2,
   numExecutors: Int = 10,
   driverMemory: String = "2g",
   maxResultSize: String = "2g"
 )
