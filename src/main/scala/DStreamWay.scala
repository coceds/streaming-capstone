import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import kafka.serializer.StringDecoder
import org.apache.commons.lang3.StringEscapeUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

import scala.collection.mutable
import scala.util.{Success, Try}
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object DStreamWay {

  def main(args: Array[String]) {
    val spark = SparkSession.builder
      .master("local[3]")
      .appName("Streaming capstone")
      .config("spark.driver.memory", "2g")
      .config("spark.cassandra.connection.host", "127.0.0.1") //todo: add host
      .getOrCreate()
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "127.0.0.1:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "capstone-group",
      "auto.offset.reset" -> "earliest", //earliest latest // "smallest", "largest"
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val checkpointDir = "data/checkpoint"
    val batch = 60
    val length = 10
    val streamingContext = StreamingContext.getOrCreate(checkpointDir, () => {
      val ssc = new StreamingContext(spark.sparkContext, Seconds(batch))
      ssc.checkpoint(checkpointDir)
      val topicsSet = Set("file-standalone")
      //      val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      //        ssc, kafkaParams, topicsSet)
      val kafkaStream = KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](topicsSet, kafkaParams)
      )
      val eventStream: DStream[Event] = kafkaStream
        .map(record => Try(convertToEvent(record.value())))
        .filter(v => v.isSuccess)
        .map(v => v.get)
        //.checkpoint(Seconds(batch * 5)) //todo: checkpoint rdd
        .filter(e => e.eventType == "click" || e.eventType == "view")


      //save to cassandra
      eventStream
        .foreachRDD(rdd => {
          if (!rdd.isEmpty()) {
            rdd.saveToCassandra("capstone", "events",
              SomeColumns("ip", "unix_time", "category_id", "eventtype" as "eventType"))
          } else {
            println("empty batch")
          }

        })

      eventStream
        .map(event => (event.ip, event))
        .mapValues(e => {
          e.eventType match {
            case "click" => ReducedEvents(1, 0, mutable.HashMap(e.category_id -> 1))
            case "view" => ReducedEvents(0, 1, mutable.HashMap(e.category_id -> 1))
          }
        })
        .reduceByKeyAndWindow(
          (txs, otherTxs) => txs.add(otherTxs), //reduce new data for last 60 seconds
          (all, old) => all.subtract(old), // inverse reduce data leaving the window (length * batch seconds)
          Seconds(batch * length),
          Seconds(batch)
        )
        .filter(key => {
          val clicks = key._2.clicks
          val views = key._2.views
          val ratio = if (views == 0) {
            clicks
          } else {
            clicks / views
          }
          val categories = key._2.categories.size
          (clicks + views > 1000) || categories > 5 || ratio > 4
        })
        .print()
      ssc
    })

    streamingContext.start
    streamingContext.awaitTermination
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

  @JsonCreator
  case class Event(@JsonProperty("unix_time") unix_time: Long,
                   @JsonProperty("category_id") category_id: Int,
                   @JsonProperty("ip") ip: String,
                   @JsonProperty("type") eventType: String)


  //clicks: Int, views: Int, categories: Map<String, Int> - key - category name, value - amount of times category is used for the interval
  case class ReducedEvents(clicks: Int, views: Int, categories: mutable.HashMap[Int, Int]) {

    def add(other: ReducedEvents): ReducedEvents = {
      other.categories
        .foreach(e => {
          this.categories.get(e._1) match {
            case Some(v) => this.categories.update(e._1, v + e._2)
            case None => this.categories += e
          }
        })
      ReducedEvents(
        this.clicks + other.clicks,
        this.views + other.views,
        this.categories
      )
    }

    def subtract(other: ReducedEvents): ReducedEvents = {
      //todo: can we use same map here?
      other.categories
        .foreach(e => {
          this.categories.get(e._1) match {
            case Some(v) => {
              v - e._2 match {
                case value if value > 0 => this.categories.update(e._1, value)
                case value if value == 0 => this.categories.remove(e._1)
              }
            }
            //case None => this.categories //this case is impossible
          }
        })
      ReducedEvents(
        this.clicks + other.clicks,
        this.views + other.views,
        this.categories
      )
    }
  }

}



