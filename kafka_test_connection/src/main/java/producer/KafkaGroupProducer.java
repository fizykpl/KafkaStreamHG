package producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import producer.model.Country;
import producer.model.Order;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Properties;

public class KafkaGroupProducer {

    public final static String TOPIC_GROUP_BUYS = "group_buys";
    public final static String BOOTSTRAP_SERVERS = "localhost:9092";
    public final static String application = "application";


    public static void main(String[] args) {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, application);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 6000);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());


        StreamsBuilder builder = new StreamsBuilder();


        Duration window = Duration.of(5, ChronoUnit.SECONDS);
        KStream<String, String> group = createGroupStream(builder, window);



        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }

    public static KStream<String, String> createGroupStream(StreamsBuilder builder, Duration window) {
        /**
         * Read topics
         */
        final KStream<String, String> buys = builder.stream(KafkaProducers.TOPIC_BUYS);
        final KTable<String, String> loginsTable = builder.table(KafkaProducers.TOPIC_LOGINS);


        ObjectMapper mapper = new ObjectMapper();

        /**
         * JOIN
         */
        KStream<String, String> join = buys.leftJoin(loginsTable,
                (buysValue, loginsValue)->parseOrder(mapper,buysValue).amount +","+parseCountry(mapper,loginsValue).country,
                Joined.keySerde(Serdes.String()));

        KStream<String, String> countryAmount = join
                .selectKey((k,v)->v.split(",")[1])
                .mapValues(v->(v.split(",")[0]));

        /**
         * GROUP
         */
        KStream<String, String> group =
                countryAmount
                        .groupByKey()
                        .windowedBy(TimeWindows.of(window).advanceBy(window))
                        .reduce((v1, v2) -> sum(v1, v2))
                        .toStream((k,v)->k.toString());
        group.to(TOPIC_GROUP_BUYS, Produced.with(Serdes.String(), Serdes.String()));

        return group;
    }

    private static Country parseCountry(ObjectMapper mapper, String jsonString) {
        jsonString = jsonString.replace("\\", "");
        if ((jsonString.charAt(1) == '{' && jsonString.charAt(jsonString.length() - 2) == '}')) {
            jsonString = jsonString.substring(1, jsonString.length() - 1);
        }

        try {
            return mapper.readValue(jsonString, Country.class);
        } catch (IOException e) {
            e.printStackTrace();
            return new Country();
        }
    }

    private static Order parseOrder(ObjectMapper mapper, String jsonString) {
        jsonString = jsonString.replace("\\", "");
        if ((jsonString.charAt(1) == '{' && jsonString.charAt(jsonString.length() - 2) == '}')) {
            jsonString = jsonString.substring(1, jsonString.length() - 1);
        }

        try {
            return mapper.readValue(jsonString, Order.class);
        } catch (IOException e) {
            e.printStackTrace();
            return new Order();
        }
    }

    private static String sum(String v1, String v2) {
        Long ret = Long.parseLong(v1) + Long.parseLong(v2);
        System.out.println(v1 + " + " + v2 + " = " + ret);
        return ret.toString();
    }
}
