package dml.kafka.producer;

import lombok.extern.slf4j.Slf4j;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.function.BiFunction;

@Slf4j
public class DemoProducer {
    private Properties props = new Properties();
    private Producer<String, String> producer;

    public DemoProducer() {
        props.setProperty("bootstrap.servers", "localhost:9093");
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
    }

    public void run() {
        for(int i = 0; i < 10; i++){
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("events", "" + i, "IoT EVENT " + i);
//            producer.send(record);
            producer.send(record, (recordMetadata, exception) -> {
                System.out.println(">>>" +
                        String.format("Topic %s, Partition: %d Offset: %d, Timestamp: %d\n",
                                recordMetadata.topic(),
                                recordMetadata.partition(),
                                recordMetadata.offset(),
                                recordMetadata.timestamp()
                        )
                );
                if (exception != null) {
                    log.error("Consumer eror: ", exception);
                }
            });
        }
    }

    public static void main(String[] args) throws InterruptedException {
        DemoProducer producer = new DemoProducer();
        producer.run();
        Thread.sleep(5000);
    }
}
