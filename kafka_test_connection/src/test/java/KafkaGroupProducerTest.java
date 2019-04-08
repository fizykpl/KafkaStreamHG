import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.Before;
import org.junit.Test;
import producer.KafkaGroupProducer;
import producer.KafkaProducers;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Properties;

public class KafkaGroupProducerTest {
    TopologyTestDriver testDriver;

    @Before
    public void setup() {

// using DSL
        StreamsBuilder builder = new StreamsBuilder();
        Duration window = Duration.of(5, ChronoUnit.SECONDS);
        KafkaGroupProducer.createGroupStream(builder, window);
        Topology topology = builder.build();

// setup test driver
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        this.testDriver = new TopologyTestDriver(topology, config);
    }

    @Test
    public void joinTest() {
        String expectedKey = "PL";
        String expectedValues = "10";

        ConsumerRecordFactory<String, String> logins = new ConsumerRecordFactory<>(KafkaProducers.TOPIC_LOGINS, new StringSerializer(), new StringSerializer());
        String user = String.format("{\"userId\":%s}", 1);
        String country = String.format("{\"country\":\"%s\"}", expectedKey);
        testDriver.pipeInput(logins.create(KafkaProducers.TOPIC_LOGINS, user, country, 1L));

        ConsumerRecordFactory<String, String> buys = new ConsumerRecordFactory<>(KafkaProducers.TOPIC_BUYS, new StringSerializer(), new StringSerializer());
        String order = String.format("{\"orderId\":\"abc\",\"amount\":%s}", expectedValues);
        testDriver.pipeInput(buys.create(KafkaProducers.TOPIC_BUYS, user, order, 1L));

        ProducerRecord<String, String> outputRecord = testDriver.readOutput(KafkaGroupProducer.TOPIC_GROUP_BUYS, new StringDeserializer(), new StringDeserializer());

        OutputVerifier.compareKeyValue(outputRecord, "PL", "10");
    }
}
