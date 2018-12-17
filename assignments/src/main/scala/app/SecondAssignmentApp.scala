package app

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}

class SecondAssignmentApp {

  /**
    * Read the topic 'pageviews' from KSQL example and count the number of events
    * per page (field 'pageid') per minute. Write the results to the 'views-per-min'
    * topic. Use unit tests to create and validate the topology and verify on generated data
    */
  def viewsPerMinute(): Topology = {
    throw new RuntimeException("not yet implemented")
  }

  object SecondAssignmentApp extends App {
    val props: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, "assignment-2-aggregate")
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
}

