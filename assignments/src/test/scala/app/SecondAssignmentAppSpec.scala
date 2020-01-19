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
//    testData.foreach(line => testDriver.pipeInput(factory.create(line)))

    // Then
//    assertValue(26)
//
//    assertResult(null)(testDriver.readOutput("views-per-min"))

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
    "{\"ROWTIME\":1579362837432,\"ROWKEY\":\"User_4\",\"USERID\":\"User_4\",\"PAGEID\":\"Page_99\",\"REGIONID\":\"Region_9\",\"GENDER\":\"FEMALE\"}",
  "{\"ROWTIME\":1579362837599,\"ROWKEY\":\"User_2\",\"USERID\":\"User_2\",\"PAGEID\":\"Page_14\",\"REGIONID\":\"Region_9\",\"GENDER\":\"FEMALE\"}",
  "{\"ROWTIME\":1579362837623,\"ROWKEY\":\"User_4\",\"USERID\":\"User_4\",\"PAGEID\":\"Page_97\",\"REGIONID\":\"Region_9\",\"GENDER\":\"FEMALE\"}",
  "{\"ROWTIME\":1579362838069,\"ROWKEY\":\"User_1\",\"USERID\":\"User_1\",\"PAGEID\":\"Page_30\",\"REGIONID\":\"Region_4\",\"GENDER\":\"FEMALE\"}",
  "{\"ROWTIME\":1579362838131,\"ROWKEY\":\"User_2\",\"USERID\":\"User_2\",\"PAGEID\":\"Page_68\",\"REGIONID\":\"Region_9\",\"GENDER\":\"FEMALE\"}",
  "{\"ROWTIME\":1579362838539,\"ROWKEY\":\"User_1\",\"USERID\":\"User_1\",\"PAGEID\":\"Page_64\",\"REGIONID\":\"Region_4\",\"GENDER\":\"FEMALE\"}",
  "{\"ROWTIME\":1579362839574,\"ROWKEY\":\"User_1\",\"USERID\":\"User_1\",\"PAGEID\":\"Page_65\",\"REGIONID\":\"Region_7\",\"GENDER\":\"FEMALE\"}",
  "{\"ROWTIME\":1579362839901,\"ROWKEY\":\"User_1\",\"USERID\":\"User_1\",\"PAGEID\":\"Page_74\",\"REGIONID\":\"Region_7\",\"GENDER\":\"FEMALE\"}",
  "{\"ROWTIME\":1579362840346,\"ROWKEY\":\"User_1\",\"USERID\":\"User_1\",\"PAGEID\":\"Page_25\",\"REGIONID\":\"Region_2\",\"GENDER\":\"FEMALE\"}"
  )

}
