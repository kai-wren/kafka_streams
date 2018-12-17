package app

import java.lang
import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{IntegerDeserializer, StringDeserializer, StringSerializer}
import org.apache.kafka.streams.test.{ConsumerRecordFactory, OutputVerifier}
import org.apache.kafka.streams.{StreamsConfig, TopologyTestDriver}
import org.scalatest.FlatSpec

class FirstAssignmentAppSpec extends FlatSpec {

  val testClass = new FirstAssignmentApp()
  val factory = new ConsumerRecordFactory[String, String]("text-input",
    new StringSerializer, new StringSerializer)

  it should "convert each line of text to the length of that text" in {
    // When
    val testDriver = new TopologyTestDriver(testClass.lineLengths(), config)
    testData.foreach(line => testDriver.pipeInput(factory.create(line)))

    // Then
    assertValue(26)
    assertValue(19)
    assertValue(58)
    assertValue(27)
    assertValue(26)

    assertResult(null)(testDriver.readOutput("line-length"))

    def assertValue(expected: Int): Unit = {
      OutputVerifier.compareValue(testDriver.readOutput("line-length",
      new StringDeserializer, new IntegerDeserializer), lang.Integer.valueOf(expected))
    }
  }

  it should "count the number of words in each line" in {
    // When
    val testDriver = new TopologyTestDriver(testClass.wordsPerLine(), config)
    testData.foreach(line => testDriver.pipeInput(factory.create(line)))

    // Then
    assertValue(4)
    assertValue(2)
    assertValue(11)
    assertValue(3)
    assertValue(4)
    assertResult(null)(testDriver.readOutput("word-count-per-line"))

    def assertValue(expected: Int): Unit = {
      OutputVerifier.compareValue(testDriver.readOutput("word-count-per-line",
        new StringDeserializer, new IntegerDeserializer), lang.Integer.valueOf(expected))
    }
  }

  it should "find the lines containing input" in {
    // Given
    val input = "hamlet"

    // When
    val testDriver = new TopologyTestDriver(testClass.linesContains(input), config)
    testData.foreach(line => testDriver.pipeInput(factory.create(line)))

    // Then
    assertValue("Hamlet, son to the former, and nephew to the present king.")
    assertValue("Horatio, friend to Hamlet.")
    assertResult(null)(testDriver.readOutput("contains-word"))

    def assertValue(expected: String): Unit = {
      OutputVerifier.compareValue(testDriver.readOutput("contains-word",
        new StringDeserializer, new StringDeserializer), expected)
    }
  }

  it should "split each line into words" in {
    // When
    val testDriver = new TopologyTestDriver(testClass.allTheWords(), config)
    testData.foreach(line => testDriver.pipeInput(factory.create(line)))

    // Then
    assertValue("Claudius")
    assertValue("King")
    assertValue("of")
    assertValue("Denmark")
    assertValue("Marcellus")
    assertValue("Officer")
    assertValue("Hamlet")
    assertValue("son")
    assertValue("to")
    assertValue("the")
    assertValue("former")
    assertValue("and")
    assertValue("nephew")
    assertValue("to")
    assertValue("the")
    assertValue("present")
    assertValue("king")
    assertValue("Polonius")
    assertValue("Lord")
    assertValue("Horatio")
    assertValue("friend")
    assertValue("to")
    assertValue("Hamlet")
    assertResult(null)(testDriver.readOutput("all-the-words"))

    def assertValue(expected: String): Unit = {
      OutputVerifier.compareValue(testDriver.readOutput("all-the-words",
        new StringDeserializer, new StringDeserializer), expected)
    }
  }

  val config: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    p
  }

  val testData = List(
    "Claudius, King of Denmark.",
    "Marcellus, Officer.",
    "Hamlet, son to the former, and nephew to the present king.",
    "Polonius, Lord Chamberlain.",
    "Horatio, friend to Hamlet."
  )
}
