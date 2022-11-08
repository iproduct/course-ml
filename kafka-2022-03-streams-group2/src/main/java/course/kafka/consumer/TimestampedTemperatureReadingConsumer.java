package course.kafka.consumer;

import course.kafka.model.TemperatureReading;
import course.kafka.model.TimestampedTemperatureReading;
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
public class TimestampedTemperatureReadingConsumer implements Runnable {
    public static final String INTERNAL_TOPIC = "internal-temperature";
    public static final String EXTERNAL_TOPIC = "external-temperature";
    public static final String CONSUMER_GROUP = "TimestampedTemperatureEventsConsumer";
    public static final String BOOTSTRAP_SERVERS = "localhost:9093";//,localhost:9094,localhost:9095";
    public static final String KEY_CLASS = "key.class";
    public static final String VALUE_CLASS = "values.class";
    public static final long POLLING_DURATION_MS = 100;

    private volatile boolean canceled;
    private String consumerGroup;
    private String topic;

    public TimestampedTemperatureReadingConsumer(String consumerGroup, String topic) {
        this.consumerGroup = consumerGroup;
        this.topic = topic;
    }

    private static Consumer<String, TemperatureReading> createConsumer(String consumerGroup) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.toString().toLowerCase());
        props.put(KEY_CLASS, String.class.getName());
        props.put(VALUE_CLASS, TimestampedTemperatureReading.class.getName());

        return new KafkaConsumer<>(props);
    }

    public void cancel() {
        canceled = true;
    }

    @Override
    public void run() {
        try (var consumer = createConsumer(consumerGroup)) {
            try {
                consumer.subscribe(List.of(topic));
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
                                log.debug("Consumer [{}] SUCCESSFULLY commited offsets: {} : {}", consumer.groupMetadata().groupId(), tp.toString(), offs.offset());
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
        TimestampedTemperatureReadingConsumer consumer1 =
                new TimestampedTemperatureReadingConsumer(CONSUMER_GROUP + "-INTERNAL", INTERNAL_TOPIC);
//        TimestampedTemperatureReadingConsumer consumer2 =
//                new TimestampedTemperatureReadingConsumer(CONSUMER_GROUP + "-EXTERNAL", EXTERNAL_TOPIC);
        var executor = Executors.newCachedThreadPool();
        var producerFuture1 = executor.submit(consumer1);
//        var producerFuture2 = executor.submit(consumer2);
        System.out.println("Hit <Enter> to close.");
        new Scanner(System.in).nextLine();
        System.out.println("Closing the consumer ...");
        consumer1.cancel();
//        consumer2.cancel();
        producerFuture1.cancel(true);
//        producerFuture2.cancel(true);

        executor.shutdown();
    }
}

