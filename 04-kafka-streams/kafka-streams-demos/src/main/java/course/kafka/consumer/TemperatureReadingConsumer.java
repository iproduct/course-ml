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
public class TemperatureReadingConsumer implements Runnable {
    public static final String INTERNAL_TEMP_TOPIC = "temperature2";
    public static final String EXTERNAL_TEMP_TOPIC = "external-temperature";
    public static final String OUTPUT_TOPIC = "temperature2";
    public static final String CONSUMER_GROUP_INTERNAL = "InternalTemperatureEventsConsumer";
    public static final String CONSUMER_GROUP_EXTERNAL = "ExternalTemperatureEventsConsumer";
    public static final String BOOTSTRAP_SERVERS = "localhost:8093";//,localhost:9094,localhost:9095";
    public static final String KEY_CLASS = "key.class";
    public static final String VALUE_CLASS = "values.class";
    public static final long POLLING_DURATION_MS = 100;

    private volatile boolean canceled;
    private final String consumerGroup;
    private final String topic;

    public TemperatureReadingConsumer(String consumerGroup, String topic) {
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
//        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.toString().toLowerCase());
        props.put(KEY_CLASS, String.class.getName());
        props.put(VALUE_CLASS, TemperatureReading.class.getName());

        // security config
//        props.put("security.protocol", "SSL");
        props.put("ssl.endpoint.identification.algorithm", "");
        props.put("ssl.truststore.location", "D:\\CourseKafka\\kafka_2.13-3.2.0\\client.truststore.jks");
        props.put("ssl.truststore.password", "changeit");
        props.put("ssl.truststore.type", "JKS");
//        props.put("ssl.truststore.certificates", "CARoot");
        props.put("ssl.enabled.protocols", "TLSv1.2,TLSv1.1,TLSv1");
        props.put("ssl.protocol", "TLSv1.2");

        // SASL PLAIN Authentication
//        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='admin' password='admin123';");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='trayan' password='trayan123';");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");

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
        TemperatureReadingConsumer consumer1 = new TemperatureReadingConsumer(CONSUMER_GROUP_INTERNAL, INTERNAL_TEMP_TOPIC);
//        TemperatureReadingConsumer consumer2 = new TemperatureReadingConsumer(CONSUMER_GROUP_EXTERNAL, EXTERNAL_TEMP_TOPIC);
        var executor = Executors.newCachedThreadPool();
        var producerFuture1 = executor.submit(consumer1);
//        var producerFuture2 = executor.submit(consumer2);
        System.out.println("Hit <Enter> to close.");
        new Scanner(System.in).nextLine();
        System.out.println("Closing the consumer ...");
        consumer1.cancel();
        producerFuture1.cancel(true);
//        consumer2.cancel();
//        producerFuture2.cancel(true);
        executor.shutdown();
    }
}

