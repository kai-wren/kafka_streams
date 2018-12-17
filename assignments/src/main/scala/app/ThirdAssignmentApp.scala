package app

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}

class ThirdAssignmentApp {

  /**
    * Join pageviews events with users state, outputing the last visited page
    * when an account is cancelled(user value equals null).
    * Use unit tests to create and validate the topology
    */
  def accountCancellationLastVisitedPage(): Topology = {
    throw new RuntimeException("not yet implemented")
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