package producer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.connect.json.JsonSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;


public class KafkaProducers {

    private final static String[][] users = {{"1", "PL"},
            {"2", "PL"},
            {"3", "CAN"},
            {"4", "USA"},
            {"5", "PL"}
    };
    private final static String jsonStringUser = "{\"userId\":%s}";
    private final static String jsonStringCountry = "{\"country\":\"%s\"}";
    private final static String jsonStringOrder = "{\"orderId\":\"%s\",\"amount\":%s}";
    public final static String TOPIC_BUYS = "buys_test_json1";
    public final static String TOPIC_LOGINS = "logins_test_json5";
    public final static String BOOTSTRAP_SERVERS = "localhost:9092";
    private static int MAX = 100;
    private static int MIN = 1;


    Producer<JsonNode, JsonNode> producer;
    ObjectMapper mapper ;

    public KafkaProducers() {
        this.producer = createProducer();
        this.mapper = new ObjectMapper();
    }


    public  Producer<JsonNode, JsonNode> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());

        props.put("parse.key", "true");
        props.put("key.separator", ":");
        return new KafkaProducer<>(props);
    }

    public void sendToLogins(JsonNode user, JsonNode county) throws InterruptedException, java.util.concurrent.ExecutionException {
        final ProducerRecord<JsonNode, JsonNode> record = new ProducerRecord<>(TOPIC_LOGINS, user, county);
        RecordMetadata metadata = producer.send(record).get();
        System.out.printf("sent record(key=%s value=%s) " +
                        "meta(partition=%d, offset=%d timestamp=%d\n)",
                record.key(), record.value(), metadata.partition(),
                metadata.offset(), metadata.timestamp());
    }

    public void sendToBuys(JsonNode user, JsonNode order) throws ExecutionException, InterruptedException {
            final ProducerRecord<JsonNode, JsonNode> record =
                    new ProducerRecord<JsonNode, JsonNode>(TOPIC_BUYS, user, order);
            RecordMetadata metadata = producer.send(record).get();
            System.out.printf("sent record(key=%s value=%s) " +
                            "meta(partition=%d, offset=%d timestamp=%d\n)",
                    record.key(), record.value(), metadata.partition(),
                    metadata.offset(), metadata.timestamp());
    }

     private void runProducerLogins() throws Exception {
        for (int i = 0; i < users.length; i++) {
            JsonNode user = createUser(users[i][0]);
            JsonNode county = createCountr(users[i][1]);
            sendToLogins(user, county);
        }
    }

    private void runProducerBuys(int msgCount) throws Exception {
        Random rn = new Random();
        for (int i = 0; i < msgCount; i++) {
            Thread.sleep(200);
            int imod = i % users.length;
            String uuid = UUID.randomUUID().toString();
            int amount = 10;//(int)(Math.random() * ( MAX - MIN ));
            String userId = users[imod][0];
            JsonNode user = createUser(userId);
            JsonNode order = createOrder(uuid, amount);
            sendToBuys(user, order);
        }
    }

    public JsonNode createUser(String userId) {
        return mapper.valueToTree(createUserJsonString(userId));
    }

    public JsonNode createCountr(String country) {
        return mapper.valueToTree(createCountryJsonString(country));
    }

    public JsonNode createOrder(String uuid, int amount) {
        return mapper.valueToTree(createOrderJsonString(uuid, amount));
    }

    String createUserJsonString(String userId) {
        return String.format(jsonStringUser, userId).replace("\\", "");
    }

    static String createCountryJsonString(String country) {
        return String.format(jsonStringCountry, country).replace("\\", "");
    }

    static String createOrderJsonString(String order, int amount) {
        return String.format(jsonStringOrder, order, amount).replace("\\", "");
    }

    public void close() {
        producer.flush();
        producer.close();
    }

    public static void main(String[] args) throws Exception {
        KafkaProducers kp = new KafkaProducers();
        kp.runProducerLogins();
        kp.runProducerBuys(500);
        kp.close();
    }

}
