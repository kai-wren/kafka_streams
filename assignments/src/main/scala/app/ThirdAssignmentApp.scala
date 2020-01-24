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
    val pageStream: KStream[String, String] = builder.stream[String, String]("pageviews") //consuming data from input topics
    val userStream: KStream[String, String] = builder.stream[String, String]("users")

    val processedUserStream = userStream.map((k, v) => {val result = JSON.parseFull(v) //parsing input data in JSON format
      result match {
        case Some(map: Map[String, Any]) => (map("userid").toString, //assign user ID as key
          Option(map("value")) match { //assign value based on "value" attribute of input JSON
            case Some(v) => map("value").toString
            case None => "null"
          })
        case None => ("null", "null")
      }
    }).groupByKey.reduce((v1, v2)=> v2).toStream.filter((k, v)=> v == "null") //grouping by key, reducing to leave only
    // latest entry and filtering out data to leave only users which are cancelled

//    processedUserStream.peek((k, v)=> println(k, v))

    val processedPageStream = pageStream.map((k, v) => {val result = JSON.parseFull(v) //parsing input data in JSON format
      result match {
        case Some(map: Map[String, Any]) => (map("userid").toString, map("pageid").toString) // creatin new key as user ID and value as page ID
        case None => ("null", "null")
      }
    }).groupByKey.reduce((v1, v2)=> v2) //grouping by key and reducing to have only latest visited page by user

//    processedPageStream.toStream.peek((k, v)=> println(k, v))

    val res = processedUserStream.leftJoin(processedPageStream)((lV: String, rV: String)=> rV)(Joined.keySerde(Serdes.String).withValueSerde(Serdes.String))
// joining both streams together to have final list of latest pages visited by cancelled users
//    res.peek((k, v)=> println(k, v))
    res.to("account-cancellation") //outputting page id to respective topic

    builder.build()

  }
}

object ThirdAssignmentApp extends App {
  val props: Properties = {
    val r = scala.util.Random
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "assignment-2-join"+r.nextInt.toString) // assigning random number due to issue on windows with dir folder is not removed on cleanup
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