package course.kafka.consumer;

import course.kafka.model.TemperatureReading;
import course.kafka.serialization.JsonDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.*;

@Slf4j
public class TemperatureConsumerGroup {
    public static final String TEMPERATURE_TOPIC = "temperature2";
    public static final String CONSUMER_GROUP_NAME = "TemperatureConsumerGroup";
    public static final int NUM_CONSUMERS = 1;
    public static final String BOOTSTRAP_SERVERS = "localhost:8093";
    public static final String KEY_CLASS = "key.class";
    public static final String VALUE_CLASS = "values.class";
    public static final long POLLING_DURATION_MS = 100;

    private final String consumerGroup;
    private final String topic;
    private ExecutorService executor;
    private Properties consumerProps = new Properties();
    private java.util.function.Consumer<Throwable> exceptionConsumer;
    private java.util.function.Consumer<TemperatureReading> temperatureConsumer;
    private List<TemperatureIndividualConsumer> consumers = new ArrayList<>();
    private List<Future<?>> consumerFutures = new ArrayList<>();

    public TemperatureConsumerGroup(String consumerGroup, String topic,
                                    java.util.function.Consumer<Throwable> exceptionConsumer,
                                    java.util.function.Consumer<TemperatureReading> temperatureConsumer) {
        this.consumerGroup = consumerGroup;
        this.topic = topic;
        executor = Executors.newCachedThreadPool();
        this.exceptionConsumer = exceptionConsumer;
        this.temperatureConsumer = temperatureConsumer;


        // init consumer props
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
//        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.toString().toLowerCase());
        consumerProps.put(KEY_CLASS, String.class.getName());
        consumerProps.put(VALUE_CLASS, TemperatureReading.class.getName());

        // security config
//        props.put("security.protocol", "SSL");
        consumerProps.put("ssl.endpoint.identification.algorithm", "");
        consumerProps.put("ssl.truststore.location", "D:\\CourseKafka\\kafka_2.13-3.2.0\\client.truststore.jks");
        consumerProps.put("ssl.truststore.password", "changeit");
        consumerProps.put("ssl.truststore.type", "JKS");
//        consumerProps.put("ssl.truststore.certificates", "CARoot");
        consumerProps.put("ssl.enabled.protocols", "TLSv1.2,TLSv1.1,TLSv1");
        consumerProps.put("ssl.protocol", "TLSv1.2");

        // SASL PLAIN Authentication
        consumerProps.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='admin' password='admin123';");
//        consumerProps.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='trayan' password='trayan123';");
        consumerProps.put("security.protocol", "SASL_SSL");
        consumerProps.put("sasl.mechanism", "PLAIN");
    }

    private Consumer<String, TemperatureReading> createKafkaConsumer() {
        return new KafkaConsumer<>(consumerProps);
    }

    public void startGroup() {
        for (int i = 0; i < NUM_CONSUMERS; i++) {
            var consumerInstance = new TemperatureIndividualConsumer(createKafkaConsumer(),
                    exceptionConsumer, temperatureConsumer, Duration.ofMillis(POLLING_DURATION_MS));
            consumers.add(consumerInstance);
            var consumerFuture = executor.submit(() -> consumerInstance.startBySubscribing(topic));
            consumerFutures.add(consumerFuture);
        }
        System.out.println("Hit <Enter> to close.");
        new Scanner(System.in).nextLine();
    }

    public void stopGroup() {
        for (int i = 0; i < NUM_CONSUMERS; i++) {
            consumers.get(i).stop();
            consumerFutures.get(i).cancel(false);
            shutdownAndAwaitTermination();
        }
    }

    private void shutdownAndAwaitTermination() {
        executor.shutdown(); // Disable new tasks from being submitted
        try {
            // Wait a while for existing tasks to terminate
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                executor.shutdownNow(); // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if (!executor.awaitTermination(30, TimeUnit.SECONDS))
                    System.err.println("Pool did not terminate");
            }
        } catch (InterruptedException ie) {
            // (Re-)Cancel if current thread also interrupted
            executor.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        TemperatureConsumerGroup consumerGroup = new TemperatureConsumerGroup(CONSUMER_GROUP_NAME, TEMPERATURE_TOPIC,
                ex -> log.error("!!! Consumer polling error: ", ex),
                temperatureReading -> log.info(">>> {}", temperatureReading));
        consumerGroup.startGroup();

        System.out.println("Hit <Enter> to close.");
        new Scanner(System.in).nextLine();
        log.info("Closing the consumer group '{}'", CONSUMER_GROUP_NAME);
        consumerGroup.stopGroup();
        log.info("Consumer group '{}' stopped successfully", CONSUMER_GROUP_NAME);
    }
}

