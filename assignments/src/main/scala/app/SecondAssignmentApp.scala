package app


import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.ImplicitConversions._
import Serdes._
import org.apache.kafka.streams.kstream.TimeWindows

import scala.util.parsing.json._

class SecondAssignmentApp {

  /**
   * Read the topic 'pageviews' from KSQL example and count the number of events
   * per page (field 'pageid') per minute. Write the results to the 'views-per-min'
   * topic. Use unit tests to create and validate the topology and verify on generated data
   */
  def viewsPerMinute(): Topology = {
    val builder: StreamsBuilder = new StreamsBuilder()
    val inputStream: KStream[String, String] = builder.stream[String, String]("pageviews")

    val processedStream = inputStream.map((k, v) => {val result = JSON.parseFull(v)
      result match {
        case Some(map: Map[String, Any]) => (map("pageid").toString, 1)
        case None => ("null", 0)
      }
    }).groupByKey

    processedStream.windowedBy(TimeWindows.of(TimeUnit.MINUTES.toMillis(1))).reduce((v1, v2)=>v1+v2)
    .toStream.map((k,v)=>(k.toString, v)).to("views-per-min")

    builder.build()

  }
}

  object SecondAssignmentApp extends App {
    val props: Properties = {
      val r = scala.util.Random
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, "assignment-2-aggregate"+r.nextInt.toString)
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      p
    }

    val app = new SecondAssignmentApp
    val topology = app.viewsPerMinute()
    println(topology.describe)

    val streams: KafkaStreams = new KafkaStreams(topology, props)
    streams.cleanUp()
    streams.start()

    sys.ShutdownHookThread {
      streams.close()
    }
  }

