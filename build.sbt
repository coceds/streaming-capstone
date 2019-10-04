name := "streaming capstone"

version := "0.1"

scalaVersion := "2.11.8"

resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.2.0",
  "org.apache.spark" % "spark-sql_2.11" % "2.2.0",
  "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.2.0",
  "org.apache.spark" % "spark-streaming_2.11" % "2.2.0",
  "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % "2.2.0",
  "org.apache.spark" % "spark-sql_2.11" % "2.2.0",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.5.3",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.2.2",
  "com.datastax.spark" % "spark-cassandra-connector_2.11" % "2.4.1"

)


