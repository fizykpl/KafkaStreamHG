import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.connect.json.JsonSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class KafkaProducers {

    private final static String[][] users =  { { "1", "PL" },
            { "2", "PL" },
            { "3", "CAN" },
            { "4", "USA" },
            { "5", "PL" }
    };
    private final static String jsonStringUser = "{\"userId\":%s}";
    private final static String jsonStringCountry = "{\"country\":\"%s\"}";
    private final static String jsonStringOrder = "{\"orderId\":\"%s\",\"amount\":%s}";
    public final static String TOPIC_BUYS = "buys_test_json";
    public final static String TOPIC_LOGINS= "logins_test_json4";
    public final static String BOOTSTRAP_SERVERS = "localhost:9092";
    private static int MAX = 100;
    private static int MIN = 1;


    private static Producer<JsonNode, JsonNode> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,JsonSerializer.class.getName());

        props.put("parse.key","true");
        props.put("key.separator",":");
        return new KafkaProducer<>(props);
    }

    static void runProducerBuys(int msgCount) throws Exception {
        final Producer<JsonNode, JsonNode> producer = createProducer();
        long time = System.currentTimeMillis();
        Random rn = new Random();
        try {
            ObjectMapper mapper = new ObjectMapper();
            for (int i = 0; i<msgCount;  i++) {
                Thread.sleep(200);
                int imod = i%users.length;
                UUID uuid = UUID.randomUUID();
                int amount = 10;//(int)(Math.random() * ( MAX - MIN ));

                JsonNode user = mapper.valueToTree(createUserJsonString(users[imod][0]));
                JsonNode order = mapper.valueToTree(createOrderJsonString(uuid.toString(),amount));

                final ProducerRecord<JsonNode, JsonNode> record =
                        new ProducerRecord<JsonNode,JsonNode>(TOPIC_BUYS, user,order);

                RecordMetadata metadata = producer.send(record).get();

                System.out.printf("sent record(key=%s value=%s) " +
                                "meta(partition=%d, offset=%d timestamp=%d\n)",
                        record.key(), record.value(), metadata.partition(),
                        metadata.offset(), metadata.timestamp());
            }
        } finally {
            producer.flush();
            producer.close();
        }
    }

    static void runProducerLogins() throws Exception {
        final Producer<JsonNode, JsonNode> producer = createProducer();
        ObjectMapper mapper = new ObjectMapper();
        try {
            for (int i = 0; i<users.length;  i++) {
                JsonNode user = mapper.valueToTree(createUserJsonString(users[i][0]));
                JsonNode county = mapper.valueToTree(createCountryJsonString(users[i][1]));
                final ProducerRecord<JsonNode, JsonNode> record = new ProducerRecord<>(TOPIC_LOGINS, user,county);

                RecordMetadata metadata = producer.send(record).get();

                System.out.printf("sent record(key=%s value=%s) " +
                                "meta(partition=%d, offset=%d timestamp=%d\n)",
                        record.key(), record.value(), metadata.partition(),
                        metadata.offset(), metadata.timestamp());
            }
        } finally {
            producer.flush();
            producer.close();
        }
    }

    static String createUserJsonString(String userId){
        return String.format(jsonStringUser,userId).replace("\\","");
    }

    static String createCountryJsonString(String country){
        return String.format(jsonStringCountry,country).replace("\\","");
    }

    static String createOrderJsonString(String order, int amount){
        return String.format(jsonStringOrder,order,amount).replace("\\","");
    }
    public static void main(String[] args) throws Exception {
        runProducerLogins();
        runProducerBuys(600);
    }

}
