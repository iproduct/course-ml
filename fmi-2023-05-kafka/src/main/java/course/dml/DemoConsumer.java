package course.dml;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
public class DemoConsumer implements Runnable {
    private Properties props = new Properties();
    KafkaConsumer<String, String> consumer;

    public DemoConsumer() {
        props.setProperty("bootstrap.servers", "localhost:9093");
        props.setProperty("group.id", "dmlConsumer");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(props);
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(Collections.singletonList("temperature"));
            while (true) {
                var records = consumer.poll(Duration.ofMillis(100));
                if (records.count() == 0) {
                    continue;
                }
                for (var r : records) {
                    System.out.printf("Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s\n",
                            r.topic(), r.partition(), r.offset(), r.key(), r.value());
                }
            }
        } catch (Exception err) {
            log.error("Error consuming temperature records:", err);
        } finally {
            consumer.close();
        }
    }
}
