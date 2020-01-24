package app

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{KStream, KTable}
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.ImplicitConversions._
import Serdes._
import org.apache.kafka.streams.kstream.{Joined, TimeWindows}


import scala.util.parsing.json.JSON

class ThirdAssignmentApp {

  /**
    * Join pageviews events with users state, outputing the last visited page
    * when an account is cancelled(user value equals null).
    * Use unit tests to create and validate the topology
    */
  def accountCancellationLastVisitedPage(): Topology = {
    val builder: StreamsBuilder = new StreamsBuilder()
    val pageStream: KStream[String, String] = builder.stream[String, String]("pageviews")
    val userStream: KStream[String, String] = builder.stream[String, String]("users")

    val processedUserStream = userStream.map((k, v) => {val result = JSON.parseFull(v)
      result match {
        case Some(map: Map[String, Any]) => (map("userid").toString,
          Option(map("value")) match {
            case Some(v) => map("value").toString
            case None => "null"
          })
        case None => ("null", "null")
      }
    }).groupByKey.reduce((v1, v2)=> v2).toStream.filter((k, v)=> v == "null")

//    processedUserStream.peek((k, v)=> println(k, v))

    val processedPageStream = pageStream.map((k, v) => {val result = JSON.parseFull(v)
      result match {
        case Some(map: Map[String, Any]) => (map("userid").toString, map("pageid").toString)
        case None => ("null", "null")
      }
    }).groupByKey.reduce((v1, v2)=> v2)

//    processedPageStream.toStream.peek((k, v)=> println(k, v))

    val res = processedUserStream.leftJoin(processedPageStream)((lV: String, rV: String)=> rV)(Joined.keySerde(Serdes.String).withValueSerde(Serdes.String)
    ).peek((k, v)=> println(k, v))



    builder.build()

  }
}

object ThirdAssignmentApp extends App {
  val props: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "assignment-2-join")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    p
  }

  val app = new ThirdAssignmentApp
  val topology = app.accountCancellationLastVisitedPage()
  println(topology.describe)

  val streams: KafkaStreams = new KafkaStreams(topology, props)
  streams.cleanUp()
  streams.start()

  sys.ShutdownHookThread {
    streams.close()
  }
}