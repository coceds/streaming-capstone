import java.io.Serializable
import java.sql.Timestamp

import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.commons.lang3.StringEscapeUtils
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator

object StructStreamWay {

  def main(args: Array[String]) {
    val spark = SparkSession.builder
      .master("local[3]")
      .appName("Streaming capstone: structured streams")
      .config("spark.driver.memory", "2g")
      .getOrCreate()

    import spark.implicits._
    val lines: Dataset[Event] = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.99.100:9092")
      .option("subscribe", "demo-1-standalone")
      .option("startingOffsets", "earliest") //earliest, latest
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .map(convertToEvent)

    val customAggregator = new CustomAggregator().toColumn.as("custom")

    val windowed = lines
      .withWatermark("unix_time", "10 minutes")
      .groupBy(
        functions.window($"unix_time", "10 minutes", "1 minutes"),
        $"ip")
      .agg(customAggregator)
      .as[AggregationResult]
      .filter(value => {
        val ratio = if (value.custom.views == 0) value.custom.clicks
        else value.custom.clicks / value.custom.views

        (value.custom.clicks + value.custom.views > 1000) || value.custom.categories.size > 5 || ratio > 4
      })

    windowed.printSchema()

    val query = windowed.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .start()

    query.awaitTermination()
  }

  class CustomAggregator() extends Aggregator[Row, AggregatedEvents, AggregatedEvents] with Serializable {

    override def zero: AggregatedEvents = AggregatedEvents(0, 0, Set())

    override def reduce(b: AggregatedEvents, row: Row): AggregatedEvents = {
      val category: Int = row.getAs("category_id")
      val eventType: String = row.getAs("eventType")
      if (eventType == "click") {
        AggregatedEvents(1, 0, Set(category))
      } else if (eventType == "view") {
        AggregatedEvents(0, 1, Set(category))
      } else {
        AggregatedEvents(0, 0, Set())
      }
    }

    override def merge(b1: AggregatedEvents, b2: AggregatedEvents): AggregatedEvents = {
      AggregatedEvents(b1.clicks + b2.clicks, b1.views + b2.views, b1.categories ++ b2.categories)
    }

    override def finish(reduction: AggregatedEvents): AggregatedEvents = {
      reduction
    }

    override def bufferEncoder: Encoder[AggregatedEvents] = Encoders.javaSerialization(classOf[AggregatedEvents])

    override def outputEncoder: Encoder[AggregatedEvents] = Encoders.javaSerialization(classOf[AggregatedEvents])
  }

  def convertToEvent(line: String): Event = {
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    var replaced = line
    if (replaced.startsWith("\"")) {
      replaced = replaced.substring(1)
    }
    if (replaced.endsWith("\"")) {
      replaced = replaced.substring(0, replaced.length - 1)
    }
    if (replaced.startsWith("[")) {
      replaced = replaced.substring(1)
    }
    if (replaced.endsWith(",")) {
      replaced = replaced.substring(0, replaced.length - 1)
    }
    if (replaced.endsWith("]")) {
      replaced = replaced.substring(0, replaced.length - 1)
    }
    replaced = StringEscapeUtils.unescapeJson(replaced)
    mapper.readValue[Event](replaced)
  }

  case class AggregationResult(ip: String, custom: AggregatedEvents)

  case class AggregatedEvents(clicks: Int, views: Int, categories: Set[Int]) {
  }

  @JsonCreator
  case class Event(@JsonProperty("unix_time") unix_time: Timestamp,
                   @JsonProperty("category_id") category_id: Int,
                   @JsonProperty("ip") ip: String,
                   @JsonProperty("type") eventType: String)

}



