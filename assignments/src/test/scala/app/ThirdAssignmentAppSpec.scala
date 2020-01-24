package app

import java.lang
import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{IntegerDeserializer, StringDeserializer, StringSerializer}
import org.apache.kafka.streams.test.{ConsumerRecordFactory, OutputVerifier}
import org.apache.kafka.streams.{StreamsConfig, TopologyTestDriver}
import org.scalatest.FlatSpec

class ThirdAssignmentAppSpec extends FlatSpec {

  val testClass = new ThirdAssignmentApp()
  val factory = new ConsumerRecordFactory[String, String]("pageviews",
    new StringSerializer, new StringSerializer)
  val factoryUser = new ConsumerRecordFactory[String, String]("users",
    new StringSerializer, new StringSerializer)

  it should "convert each line of text to the length of that text" in {
    // When
    val testDriver = new TopologyTestDriver(testClass.accountCancellationLastVisitedPage(), config)
    testData1.foreach(line =>testDriver.pipeInput(factory.create(line)))
    testData2.foreach(line =>testDriver.pipeInput(factory.create(line)))
    testDataUser.foreach(line =>testDriver.pipeInput(factoryUser.create(line)))


////     Then first execution within a minute
//    assertValue(1)
//    assertValue(1)
//    assertValue(1)
//    assertValue(2)
//    assertValue(2)
//    assertValue(3)
////
//    assertValue(4)
//    assertValue(5)
//    assertValue(6)

//    //     Then second execution within a minute
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
//    assertResult(null)(testDriver.readOutput("account-cancellation"))

    def assertValue(expected: Int): Unit = {
      OutputVerifier.compareValue(testDriver.readOutput("account-cancellation",
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
    "{\"viewtime\": \"1579362838000\", \"userid\": \"User_6\", \"pageid\": \"Page_2\" }"
  )
  val testData2 = List(
  "{\"viewtime\": \"1579362848000\", \"userid\": \"User_5\", \"pageid\": \"Page_2\" }",
    "{\"viewtime\": \"1579362858000\", \"userid\": \"User_3\", \"pageid\": \"Page_3\" }",
    "{\"viewtime\": \"1579362868000\", \"userid\": \"User_6\", \"pageid\": \"Page_1\" }"
  )

  val testDataUser = List(
    "{\"userid\": \"User_1\", \"value\": {\" firstname\": \"Harley\", \"lastname\": \"Queen\"}, \"gender\": \"Female\" }",
    "{\"userid\": \"User_2\", \"value\": {\" firstname\": \"Jocker\", \"lastname\": \"Jester\"}, \"gender\": \"Male\" }",
    "{\"userid\": \"User_3\", \"value\": null, \"gender\": \"Female\" }",
    "{\"userid\": \"User_4\", \"value\": {\" firstname\": \"Bat\", \"lastname\": \"Man\"}, \"gender\": \"Male\" }",
    "{\"userid\": \"User_5\", \"value\": {\" firstname\": \"Cat\", \"lastname\": \"Woman\"}, \"gender\": \"Female\" }",
    "{\"userid\": \"User_6\", \"value\": {\" firstname\": \"Super\", \"lastname\": \"Man\"}, \"gender\": \"Male\" }",
    "{\"userid\": \"User_6\", \"value\": null, \"gender\": \"Male\" }"
  )

}


