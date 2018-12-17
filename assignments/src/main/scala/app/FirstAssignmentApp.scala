package app

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}

class FirstAssignmentApp {

  /**
    * Read the Kafka topic 'text', convert each line of text to the
    * length of that text and send it to the topic 'line-lengths'
    * as a stream of ints
    */
  def lineLengths(): Topology = {
    throw new RuntimeException("not yet implemented")
  }

  /**
    * Read the Kafka topic 'text', count the number of words in
    * each line and send that to the topic 'words-per-line' as a
    * stream of ints
    */
  def wordsPerLine(): Topology = {
    throw new RuntimeException("not yet implemented")
  }

  /**
    * Read the Kafka topic 'text', find the lines containing input word
    * and send them to the topic 'contains-word'
    */
  def linesContains(word: String): Topology = {
    throw new RuntimeException("not yet implemented")
  }

  /**
    * Read the Kafka topic 'text', split each line into words and
    * send them individually to the topic 'all-the-words'
    */
  def allTheWords(): Topology = {
    throw new RuntimeException("not yet implemented")
  }
}

object FirstAssignmentApp extends App {
  val props: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "assignment-1-transform")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    p
  }

  if (args.length == 0) {
    printUsage()
  }

  val app = new FirstAssignmentApp
  val topology = args(0) match {
    case "1" => app.lineLengths()
    case "2" => app.wordsPerLine()
    case "3" => app.linesContains(args(1))
    case "4" => app.allTheWords()
    case _ =>
      printUsage()
      sys.exit(-1)
  }
  println(topology.describe)

  val streams: KafkaStreams = new KafkaStreams(topology, props)
  streams.cleanUp()
  streams.start()

  sys.ShutdownHookThread {
    streams.close()
  }

  private def printUsage(): Unit = {
    println("Usage: java -cp assignment-1-transform.jar app.FirstAssignmentApp <exercise number> <input params>")
  }
}