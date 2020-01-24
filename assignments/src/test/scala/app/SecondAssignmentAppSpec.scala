package app

import java.lang
import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{IntegerDeserializer, IntegerSerializer, StringDeserializer, StringSerializer}
import org.apache.kafka.streams.test.{ConsumerRecordFactory, OutputVerifier}
import org.apache.kafka.streams.{StreamsConfig, TopologyTestDriver}
import org.scalatest.FlatSpec

class SecondAssignmentAppSpec extends FlatSpec {

  val testClass = new SecondAssignmentApp()
  val factory = new ConsumerRecordFactory[String, String]("pageviews",
    new StringSerializer, new StringSerializer) //creating factory for topic pageviews

  it should "count number of visits per page within a one minute long time intervals" in {
    // When
    val testDriver = new TopologyTestDriver(testClass.viewsPerMinute(), config)
    testData1.foreach(line =>testDriver.pipeInput(factory.create(line))) //feeding data to input streams
    testData2.foreach(line =>testDriver.pipeInput(factory.create(line)))


//     Then first execution within a minute
    // I wasn't able to simulate course of time during testing. Threads.sleep() doesn't help either.
    // Hence I was able to validate expected behavior by running test multiple times within a same minute.
    // For this reason I am providing assert values for first and second run. So that two subsequent runs could be used to perform full testing
    // To be commented for first run
    assertValue(1)
    assertValue(1)
    assertValue(1)
    assertValue(2)
    assertValue(2)
    assertValue(3)
//
    assertValue(4)
    assertValue(5)
    assertValue(6)

//    //     Then second execution within a minute
    // To be uncommented for second run
//    assertValue(3)
//    assertValue(7)
//    assertValue(2)
//    assertValue(4)
//    assertValue(8)
//    assertValue(9)
//    //
//    assertValue(10)
//    assertValue(11)
//    assertValue(12)
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
    p
  }

  val testData1 = List(
  "{\"viewtime\": \"1579362788000\", \"userid\": \"User_1\", \"pageid\": \"Page_1\" }",
    "{\"viewtime\": \"1579362798000\", \"userid\": \"User_1\", \"pageid\": \"Page_2\" }",
    "{\"viewtime\": \"1579362808000\", \"userid\": \"User_2\", \"pageid\": \"Page_3\" }",
    "{\"viewtime\": \"1579362818000\", \"userid\": \"User_3\", \"pageid\": \"Page_1\" }",
    "{\"viewtime\": \"1579362828000\", \"userid\": \"User_4\", \"pageid\": \"Page_2\" }",
    "{\"viewtime\": \"1579362838000\", \"userid\": \"User_2\", \"pageid\": \"Page_2\" }"
  )
  val testData2 = List(
  "{\"viewtime\": \"1579362848000\", \"userid\": \"User_2\", \"pageid\": \"Page_2\" }",
    "{\"viewtime\": \"1579362858000\", \"userid\": \"User_2\", \"pageid\": \"Page_2\" }",
    "{\"viewtime\": \"1579362868000\", \"userid\": \"User_2\", \"pageid\": \"Page_2\" }"
  )

}


