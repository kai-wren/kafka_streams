package app

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.kstream.{Consumed, Produced}
import org.apache.kafka.streams.scala.ImplicitConversions._
import Serdes._
import org.apache.kafka.common.serialization.{IntegerSerializer, StringDeserializer, StringSerializer}
import org.apache.kafka.streams.processor.{Processor, ProcessorContext, ProcessorSupplier, PunctuationType}
import org.apache.kafka.streams.state.{KeyValueStore, StoreBuilder, Stores}

//object lineLengthProcessorSupplier extends ProcessorSupplier[String, String] {
//  override def get(): Processor[String, String] = new Processor[String, String] {
//    var keyValueStore: KeyValueStore[String, Int] = null
//
//    override def init(context: ProcessorContext): Unit = {
//      keyValueStore = context.getStateStore("textStore").asInstanceOf[KeyValueStore[String, Int]]
//    }
//
//    override def process(key: String, value: String): Unit = {
//      keyValueStore.put(key, value.length)
//    }
//
//    override def close(): Unit = {}
//  }
//
//}


class FirstAssignmentApp {

  /**
    * Read the Kafka topic 'text', convert each line of text to the
    * length of that text and send it to the topic 'line-lengths'
    * as a stream of ints
    */
  def lineLengths(): Topology = {
    val builder: StreamsBuilder = new StreamsBuilder()
    val inputText: KStream[String, String] = builder.stream[String, String]("text-input")
    val textLength: KStream[String, Int] = inputText.mapValues(_.length())
    val outputLength = textLength.to("line-length")
    builder.build()

//    val storeTemplate: StoreBuilder[KeyValueStore[String, Int]] = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("textStore"), Serdes.String, Serdes.Integer).withLoggingDisabled()
//    val store: KeyValueStore[String, Int] = storeTemplate.build()
//
//    val topology: Topology = new Topology();
//    topology
//      .addSource("Input", new StringDeserializer, new StringDeserializer, "text-input")
//      .addProcessor("calcLength", lineLengthProcessorSupplier, "Input")
//      .addStateStore(storeTemplate, "calcLength")
//      .addSink("Output", "line-length", new StringSerializer, new IntegerSerializer, "calcLength")
  }

  /**
    * Read the Kafka topic 'text', count the number of words in
    * each line and send that to the topic 'words-per-line' as a
    * stream of ints
    */
  def wordsPerLine(): Topology = {
    val builder: StreamsBuilder = new StreamsBuilder()
    val inputText: KStream[String, String] = builder.stream[String, String]("text-input")
    val wordsCount: KStream[String, Int] = inputText.mapValues(_.toLowerCase()).mapValues(_.split("\\W+").length)
    val outputCount = wordsCount.to("word-count-per-line")
    builder.build()
  }

  /**
    * Read the Kafka topic 'text', find the lines containing input word
    * and send them to the topic 'contains-word'
    */
  def linesContains(word: String): Topology = {
    val builder: StreamsBuilder = new StreamsBuilder()
    val inputText: KStream[String, String] = builder.stream[String, String]("text-input")
    val containsWord: KStream[String, String] = inputText.filter((_, v) => v.toLowerCase.contains(word.toLowerCase()))
    val outputLine = containsWord.to("contains-word")
    builder.build()
  }

  /**
    * Read the Kafka topic 'text', split each line into words and
    * send them individually to the topic 'all-the-words'
    */
  def allTheWords(): Topology = {
    val builder: StreamsBuilder = new StreamsBuilder()
    val inputText: KStream[String, String] = builder.stream[String, String]("text-input")
    val allWords: KStream[String, String] = inputText.flatMapValues(_.split("\\W+"))
    val outputWords = allWords.to("all-the-words")
    builder.build()
  }
}

object FirstAssignmentApp extends App {
  val props: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "assignment-1-transform")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
//    p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
//    p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
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