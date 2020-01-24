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

    val processedStream = inputStream.map((k, v) => {val result = JSON.parseFull(v) //parsing info from JSON format
      result match {
        case Some(map: Map[String, Any]) => (map("pageid").toString, 1) //remapping key and putting 1 as value
        case None => ("null", 0)
      }
    }).groupByKey //grouping data by new key - pageid

    processedStream.windowedBy(TimeWindows.of(TimeUnit.MINUTES.toMillis(1))).reduce((v1, v2)=>v1+v2) //performing windowed reduce of grouped data to count number of events within a minute.
    .toStream.map((k,v)=>(k.toString, v)).to("views-per-min") //feeding data to output topic

    builder.build() //building topology

  }
}

  object SecondAssignmentApp extends App {
    val props: Properties = {
      val r = scala.util.Random
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, "assignment-2-aggregate"+r.nextInt.toString) // assigning random number due to issue on windows with dir folder is not removed on cleanup
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

