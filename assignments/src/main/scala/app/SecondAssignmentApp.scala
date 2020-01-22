package app


import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{KStream, KTable}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.ImplicitConversions._
import Serdes._
import org.apache.kafka.streams.kstream.{TimeWindows, Window}

import scala.util.parsing.json._
//import io.circe.generic.auto._
import org.apache.kafka.streams.kstream.GlobalKTable
import org.apache.kafka.streams.state.{KeyValueStore, StoreBuilder, Stores}

class SecondAssignmentApp {

  /**
   * Read the topic 'pageviews' from KSQL example and count the number of events
   * per page (field 'pageid') per minute. Write the results to the 'views-per-min'
   * topic. Use unit tests to create and validate the topology and verify on generated data
   */
  def viewsPerMinute(): Topology = {
    val builder: StreamsBuilder = new StreamsBuilder()
    val inputStream: KStream[String, String] = builder.stream[String, String]("pageviews")

//    inputStream.peek( (_,v)=> { val result = JSON.parseFull(v)
//    result match {
//      case Some(map: Map[String, Any]) => println(map.get("pageid"))
//      case None => println("Parsing failed")
//      case other => println("Unknown data structure: " + other)
//      }
//    } )

//    inputStream.mapValues(v => {val result = JSON.parseFull(v)
//      result match {
//        case Some(map: Map[String, Any]) => PageView(map("userid").toString, map("pageid").toString, map("viewtime").toString.toLong)
//        case None => PageView("none", "none", 0)
//      }
//    }).groupBy((key, PV) => PV.page)

    val processedStream = inputStream.map((k, v) => {val result = JSON.parseFull(v)
      result match {
        case Some(map: Map[String, Any]) => (map("pageid").toString, 1)
        case None => ("none", 1)
      }
    }).groupByKey

//    processedStream.reduce((v1, v2)=> v1+v2).toStream.peek((k, v)=> println(k, v))
processedStream.windowedBy(TimeWindows.of(TimeUnit.MINUTES.toMillis(1))).reduce((v1, v2)=>v1+v2)
  .toStream.map((k,v)=>(k.toString, v)).peek((k, v)=> println(k, v)).to("views-per-min")


//    val table = inputStream.groupBy((key, value) -> value.).reduce((_, v) => v)
//    table.toStream.to("views-per-min")
//    inputStream.to("views-per-min")




    //    val countsStore: StoreBuilder[KeyValueStore[String, Int]] = Stores.keyValueStoreBuilder(
    //      Stores.persistentKeyValueStore("WordCountsStore"),
    //      Serdes.String,
    //      Serdes.Integer)
    //      .withCachingEnabled()
    //
    //    val store = builder.addStateStore(countsStore);

    //    val pageCount = table.groupBy()
    //    val viewsPerMinute: KStream[String, Int] = inputText.mapValues( )
    //    val outputLength = inputStream.to("views-per-min")
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

