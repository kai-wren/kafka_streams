package app

import java.lang
import java.util.Properties

import com.fasterxml.jackson.annotation.JsonValue
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{IntegerDeserializer, IntegerSerializer, StringDeserializer, StringSerializer}
import org.apache.kafka.streams.test.{ConsumerRecordFactory, OutputVerifier}
import org.apache.kafka.streams.{StreamsConfig, TopologyTestDriver}
import org.scalatest.FlatSpec

class SecondAssignmentAppSpec extends FlatSpec {

  val testClass = new SecondAssignmentApp()
  val factory = new ConsumerRecordFactory[String, String]("pageviews",
    new StringSerializer, new StringSerializer)

  it should "convert each line of text to the length of that text" in {
    // When
    val testDriver = new TopologyTestDriver(testClass.viewsPerMinute(), config)
    testData.foreach(line => {testDriver.pipeInput(factory.create(line))
      Thread.sleep(10000)
    })

    // Then
    assertValue(1)
    assertValue(1)
    assertValue(1)
    assertValue(2)
    assertValue(2)
    assertValue(3)
//
    assertValue(4)
    assertResult(null)(testDriver.readOutput("views-per-min"))

    def assertValue(expected: Int): Unit = {
      OutputVerifier.compareValue(testDriver.readOutput("views-per-min",
      new StringDeserializer, new IntegerDeserializer), lang.Integer.valueOf(expected))
    }
  }

  val config: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app2")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
//    p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
//    p.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    p
  }

  val testData = List(
  "{\"viewtime\": \"1579362788000\", \"userid\": \"User_1\", \"pageid\": \"Page_1\" }",
    "{\"viewtime\": \"1579362798000\", \"userid\": \"User_1\", \"pageid\": \"Page_2\" }",
    "{\"viewtime\": \"1579362808000\", \"userid\": \"User_2\", \"pageid\": \"Page_3\" }",
    "{\"viewtime\": \"1579362818000\", \"userid\": \"User_3\", \"pageid\": \"Page_1\" }",
    "{\"viewtime\": \"1579362828000\", \"userid\": \"User_4\", \"pageid\": \"Page_2\" }",
    "{\"viewtime\": \"1579362838000\", \"userid\": \"User_2\", \"pageid\": \"Page_2\" }",
  "{\"viewtime\": \"1579362838000\", \"userid\": \"User_2\", \"pageid\": \"Page_2\" }"
  )

}
