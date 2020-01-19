package app

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{KStream, KTable}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.ImplicitConversions._
import Serdes._
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
    inputStream.foreach((_,v) => println(v))
    println("test2")
//    val table = inputStream.groupByKey.reduce((_, v) -> v)

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

  object SecondAssignmentApp extends App {
    val props: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, "assignment-2-aggregate")
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
//      p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
//      p.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
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
}

