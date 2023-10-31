package course.kafka.consumer;

import course.kafka.model.TemperatureReading;
import course.kafka.serialization.JsonDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

@Slf4j
public class TemperatureReadingConsumer implements Runnable {
    public static final String TOPIC = "temperature";
    public static final String CONSUMER_GROUP = "TemperatureEventsConsumer";
    public static final String BOOTSTRAP_SERVERS = "localhost:9093";//,localhost:9094,localhost:9095";
    public static final String KEY_CLASS = "key.class";
    public static final String VALUE_CLASS = "values.class";
    public static final long POLLING_DURATION_MS = 100;

    private volatile boolean canceled;

    private static Consumer<String, TemperatureReading> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
//        props.put(ConsumerConfig.DEFAULT_ISOLATION_LEVEL, IsolationLevel.READ_COMMITTED);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.toString().toLowerCase());
        props.put(KEY_CLASS, String.class.getName());
        props.put(VALUE_CLASS, TemperatureReading.class.getName());

        return new KafkaConsumer<>(props);
    }

    public void cancel() {
        canceled = true;
    }

    @Override
    public void run() {
        try (var consumer = createConsumer()) {
            try {
                consumer.subscribe(List.of(TOPIC));
                while (!canceled) {
                    var records = consumer.poll(
                            Duration.ofMillis(POLLING_DURATION_MS));
                    if (records.count() == 0) continue;
                    for (var r : records) {
                        log.info("[Topic: {}, Partition: {}, Offset: {}, Timestamp: {}, Leader Epoch: {}]: {} -->\n    {}",
                                r.topic(), r.partition(), r.offset(), r.timestamp(), r.leaderEpoch(), r.key(), r.value());
                    }
                    consumer.commitAsync((offsets, exception) -> {
                        if (exception != null) {
                            log.error("Consumer [" + consumer.groupMetadata().groupId() + "] FAILED to commit offsets: " + offsets, exception);
                        } else {
                            offsets.forEach((tp, offs) -> {
                                log.info("Consumer [{}] SUCCESSFULLY commited offsets: {} : {}", consumer.groupMetadata().groupId(), tp.toString(), offs.offset());
                            });
                        }
                    });
                }
            } catch (Exception ex) {
                log.error("Consumer [" + consumer.groupMetadata().groupId() + "] FAILED to commit offsets.", ex);
            } finally {
                consumer.commitSync();
            }
        }
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        TemperatureReadingConsumer consumer = new TemperatureReadingConsumer();
        var executor = Executors.newCachedThreadPool();
        var producerFuture = executor.submit(consumer);
        System.out.println("Hit <Enter> to close.");
        new Scanner(System.in).nextLine();
        System.out.println("Closing the consumer ...");
        consumer.cancel();
        producerFuture.cancel(true);
        executor.shutdown();
    }
}

