package course.kafka;

import course.kafka.model.Customer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.simple.JSONObject;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class DemoConsumer {
    private Properties props = new Properties();
    private KafkaConsumer<String, Customer> consumer;
    private Map<String, Integer> eventMap = new ConcurrentHashMap<>();

    public DemoConsumer() {
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "event-consumer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "course.kafka.serialization.JsonDeserializer");
        props.put("value.deserializer.class", "course.kafka.model.Customer");
        consumer = new KafkaConsumer<>(props);
    }

    public void run() {
        consumer.subscribe(Collections.singletonList("events"));
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
        DemoConsumer demoConsumer = new DemoConsumer();
        demoConsumer.run();
    }
}
