package course.dml;

import course.dml.model.TemperatureReading;
import course.dml.serialization.JsonDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
public class DemoConsumer implements Runnable {
    public static final String BOOTSTRAP_SERVERS = "localhost:9093";//,localhost:9094,localhost:9095";
    public static final String KEY_CLASS = "key.class";
    public static final String VALUE_CLASS = "values.class";
    public static final long POLLING_DURATION_MS = 100;
    private Properties props = new Properties();
    KafkaConsumer<String, TemperatureReading> consumer;

    public DemoConsumer() {
        props.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.setProperty("group.id", "dmlConsumer");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", JsonDeserializer.class.getName());
        props.setProperty(KEY_CLASS, String.class.getName());
        props.setProperty(VALUE_CLASS, TemperatureReading.class.getName());
        consumer = new KafkaConsumer<>(props);
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(Collections.singletonList("temperature"));
            while (true) {
                var records = consumer.poll(Duration.ofMillis(POLLING_DURATION_MS));
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

    public static void main(String[] args) {
        var consumer = new DemoConsumer();
        consumer.run();
    }
}
