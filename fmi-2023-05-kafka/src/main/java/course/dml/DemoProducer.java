package course.dml;

import course.dml.partitioner.TemperatureReadingsPartitioner;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;

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
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, TemperatureReadingsPartitioner.class.getName());
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
