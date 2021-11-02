package course.kafka;

import course.kafka.model.Customer;
import course.kafka.serialization.JsonDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.simple.JSONObject;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class SecureDemoConsumer {
    private Properties props = new Properties();
    private KafkaConsumer<String, Customer> consumer;
    private Map<String, Integer> eventMap = new ConcurrentHashMap<>();

    public SecureDemoConsumer() {
        props.put("bootstrap.servers", "localhost:8092");
        // security config
        props.put("security.protocol", "SSL");
        props.put("ssl.endpoint.identification.algorithm", "");
        props.put("ssl.truststore.location", "D:\\CourseKafka\\kafka_2.12-2.2.1\\client.truststore.jks");
        props.put("ssl.truststore.password", "changeit");
        props.put("ssl.truststore.type", "JKS");
        // other config
        props.put("group.id", "event-consumer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "course.kafka.serialization.JsonDeserializer");
        props.put("value.deserializer.class", "course.kafka.model.Customer");
        consumer = new KafkaConsumer<>(props);
    }

    public void run() {
        consumer.subscribe(Collections.singletonList("test2"));
        while(true) {
            ConsumerRecords<String, Customer> records = consumer.poll(Duration.ofMillis(100));
            if(records.count() > 0) {
                log.info("Fetched {} records:", records.count());
                for (ConsumerRecord<String, Customer> rec : records) {
                    log.debug("Record - topic: {}, partition: {}, offset: {}, timestamp: {}, value: {}.",
                            rec.topic(), rec.partition(), rec.offset(), rec.timestamp(), rec.value());
                    log.info("{} -> {}, {}, {}, {}", rec.key(),
                            rec.value().getId(),
                            rec.value().getName(),
                            rec.value().getEik(),
                            rec.value().getAddress());
                    int updatedCount = 1;
                    if(eventMap.containsKey(rec.key())) {
                        updatedCount = eventMap.get(rec.key()) + 1;
                    }
                    eventMap.put(rec.key(), updatedCount);
                }
                JSONObject json = new JSONObject(eventMap);
                log.info(json.toJSONString());
            }
        }
    }

    public static void main(String[] args) {
        SecureDemoConsumer demoConsumer = new SecureDemoConsumer();
        demoConsumer.run();
    }
}
