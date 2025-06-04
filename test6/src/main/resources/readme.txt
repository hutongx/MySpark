gemini回答链接:
https://gemini.google.com/app/a477fc72a834bc97?hl=zh-TW#:~:text=Okay%2C%20I%20can%20certainly,reference%20for%20your%20project.

Okay, I can certainly help you with that! Building a robust, industrial-grade data warehouse ETL pipeline with SparkSQL, Scala, and Maven for YARN execution involves several key components.
I'll provide you with a structured set of code examples and explanations that you can use as a reference for your project.

We'll cover:
- The Maven pom.xml for project setup and dependencies.
- A recommended project directory structure.
- Configuration management using Typesafe Config (HOCON).
- A utility for creating and managing Spark sessions.
- Example jobs for ODS to DWD (cleaning) and DWD to DWS (aggregation) layers, using external SQL files.
- A main application orchestrator.
- Logging configuration.
- An example of how to submit the application to YARN, along with important production considerations.
