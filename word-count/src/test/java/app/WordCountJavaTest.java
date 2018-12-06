package app;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

public class WordCountJavaTest {

    private TopologyTestDriver testDriver;

    @Before
    public void setUp(){
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        testDriver  = new TopologyTestDriver(WordCountJava.createTopology(), config);
    }

    @After
    public void tearDown() {
        testDriver.close();
    }

    @Test
    public void createTopology(){
        ConsumerRecordFactory<String, String> factory = new ConsumerRecordFactory<>(
            "word-count-input", new StringSerializer(), new StringSerializer()
        );

        testDriver.pipeInput(factory.create("word1 word2 word3"));

        OutputVerifier.compareKeyValue(readOutput(), "word1", 1L);
        OutputVerifier.compareKeyValue(readOutput(), "word2", 1L);
        OutputVerifier.compareKeyValue(readOutput(), "word3", 1L);

        testDriver.pipeInput(factory.create("word2 word3"));

        OutputVerifier.compareKeyValue(readOutput(), "word2", 2L);
        OutputVerifier.compareKeyValue(readOutput(), "word3", 2L);

        testDriver.pipeInput(factory.create("word3"));

        OutputVerifier.compareKeyValue(readOutput(), "word3", 3L);

    }

    private ProducerRecord<String, Long> readOutput() {
        return testDriver.readOutput(
            "word-count-output", new StringDeserializer(), new LongDeserializer()
        );
    }
}
