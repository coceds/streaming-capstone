name := "streaming capstone"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.2.0",
  "org.apache.spark" % "spark-sql_2.11" % "2.2.0",
  "org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % "2.2.0",
  "org.apache.spark" % "spark-streaming_2.11" % "2.2.0",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.5.3",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.2.2"

)


