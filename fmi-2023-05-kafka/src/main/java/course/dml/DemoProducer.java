package course.dml;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.util.Properties;

public class DemoProducer {
    private Properties props = new Properties();
    private Producer<String, String> producer;

    public DemoProducer() {
        props.setProperty("bootstrap.servers", "localhost:9093");
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
    }
}
