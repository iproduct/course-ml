package course.dml;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Slf4j
public class DemoProducer implements Runnable {
    private Properties props = new Properties();
    private Producer<String, String> producer;

    private int numReadings = 10;

    public DemoProducer(int numReadings) {
        this.numReadings = numReadings;
        props.setProperty("bootstrap.servers", "localhost:9093");
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
    }


    @Override
    public void run() {
        var latch = new CountDownLatch(numReadings);
        for (int i = 0; i < numReadings; i++) {
            var record = new ProducerRecord<String, String>("temperature", "sensor-0" + (i % 2 + 1), 18 + i * 0.5 + "");
//                sendResult = producer.send(record).get();
            int finalI = i;
            producer.send(record, (result, exception) -> {
                if (exception != null) {
                    log.error("Error producing record:", exception);
                } else {
                    System.out.printf("Topic: %s, Partition: %d, Offset: %d, Timestamp: %d, TEMPERATURE: %s\n",
                            result.topic(), result.partition(), result.offset(), result.timestamp(), 18 + finalI * 0.5 + "");
                }
                latch.countDown();
            });
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                log.warn("Error producing record:", e);
                throw new RuntimeException(e);
            }
        }
        try {
            latch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.error("Error producing record:", e);
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        var producer = new DemoProducer(100);
        producer.run();
    }
}
