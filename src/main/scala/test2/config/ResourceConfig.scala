package test2.config

case class ResourceConfig(
   sparkConfig: Map[String, String],
   hiveConfig: Map[String, String],
   hdfsConfig: Map[String, String],
   resourceLimits: ResourceLimits
 )
