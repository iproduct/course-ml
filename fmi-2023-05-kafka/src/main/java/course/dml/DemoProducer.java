package course.dml;

import course.dml.model.TemperatureReading;
import course.dml.partitioner.TemperatureReadingsPartitioner;
import course.dml.serialization.JsonSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.LocalDateTime;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Slf4j
public class DemoProducer implements Runnable {
    private Properties props = new Properties();
    private Producer<String, TemperatureReading> producer;

    private int numReadings = 10;

    public DemoProducer(int numReadings) {
        this.numReadings = numReadings;
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, TemperatureReadingsPartitioner.class.getName());
        producer = new KafkaProducer<>(props);
    }


    @Override
    public void run() {
        var latch = new CountDownLatch(numReadings);
        for (int i = 0; i < numReadings; i++) {
            var sensor = "sensor-0" + (i % 2 + 1);
            var record = new ProducerRecord<String, TemperatureReading>("temperature", sensor,
                    new TemperatureReading(i + "", sensor,  18 + i * 0.5, LocalDateTime.now()));
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
