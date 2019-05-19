import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import kafka.serializer.StringDecoder
import org.apache.commons.lang3.StringEscapeUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

object DStreamWay {

  def main(args: Array[String]) {
    val spark = SparkSession.builder
      .master("local[3]")
      .appName("Streaming capstone")
      .config("spark.driver.memory", "2g")
      //.enableHiveSupport
      .getOrCreate()
    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> "192.168.99.100:9092",
      "group.id" -> "capstone-group",
      "auto.offset.reset" -> "smallest" // "smallest", "largest"
    )

    val checkpointDir = "data/checkpoint"
    val batch = 60
    val length = 10
    val streamingContext = StreamingContext.getOrCreate(checkpointDir, () => {
      val ssc = new StreamingContext(spark.sparkContext, Seconds(batch))
      ssc.checkpoint(checkpointDir)
      //createStream(ssc, "192.168.99.100:2181", "capstone-group", Map("demo-1-standalone" -> 3))
      val topicsSet = Set("demo-1-standalone")
      val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaParams, topicsSet)
      kafkaStream
        .map(record => convertToEvent(record._2))
        //filter events here if required
        //.checkpoint(Seconds(batch * 5)) //todo: checkpoint rdd
        .map(event => (event.ip, event))
        .mapValues(e => List(e))
        .reduceByKeyAndWindow((txs, otherTxs) => txs ++ otherTxs, (all, old) => all diff old, Seconds(batch * length), Seconds(batch))
        .mapWithState(
          StateSpec.function((ip: String, newEventsOpt: Option[List[Event]],
                              aggData: State[AggregatedEvents]) => {
            val newEvents = newEventsOpt.getOrElse(List.empty[Event])

            //todo: calculate these from newEvents!
            val clicks = 1
            val views = 1
            val categories = Set(1, 2, 3)

            val oldAggDataOption = aggData.getOption
            aggData.update(
              oldAggDataOption match {
                case Some(value) => {
                  if (value.size == 10) {
                    //todo: remove oldest element first
                    AggregatedEvents(value.size, value.clicks :: List(clicks), value.views :: List(views), value.categories :: List(categories))
                  } else {
                    AggregatedEvents(value.size + 1, value.clicks :: List(clicks), value.views :: List(views), value.categories :: List(categories))
                  }
                }
                case None => AggregatedEvents(1, List(clicks), List(views), List(categories))
              }
            )
          }))
        .stateSnapshots
        .filter(key => {
          val clicks = key._2.clicks.sum
          val views = key._2.views.sum
          val ratio = if (views == 0) {
            clicks
          } else {
            clicks / views
          }
          val categories = key._2.categories.flatten.size
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

  case class AggregatedEvents(size: Int, clicks: List[Int], views: List[Int], categories: List[Set[Int]]) {
  }

}



