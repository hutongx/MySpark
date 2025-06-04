package org.my_spark.utils

import org.slf4j.LoggerFactory
import scala.io.Source
import scala.util.{Try, Using}

object FileUtil {
    private val logger = LoggerFactory.getLogger(this.getClass)

    /**
     * Reads a resource file (e.g., an SQL script) from the classpath.
     * @param resourcePath Path relative to the resources directory (e.g., "sql/ods_to_dwd/my_query.sql")
     * @return String content of the file, or an empty string if not found or error.
     */
    def readResourceFile(resourcePath: String): String = {
        logger.debug(s"Attempting to read resource file: $resourcePath")
        Try {
            Using(Source.fromInputStream(getClass.getClassLoader.getResourceAsStream(resourcePath))) { source =>
                source.mkString
            }.get
        }.fold(
            error => {
                logger.error(s"Failed to read resource file '$resourcePath': ${error.getMessage}", error)
                "" // Return empty or throw exception based on desired error handling
            },
            content => {
                logger.info(s"Successfully read resource file: $resourcePath")
                content
            }
        )
    }
}
