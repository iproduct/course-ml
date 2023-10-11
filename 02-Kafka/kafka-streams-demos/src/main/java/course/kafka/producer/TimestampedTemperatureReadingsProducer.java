package course.kafka.producer;

import course.kafka.interceptor.CountingProducerInterceptor;
import course.kafka.model.TimestampedTemperatureReading;
import course.kafka.partitioner.TemperatureReadingsPartitionerBySensorId;
import course.kafka.serialization.JsonSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static course.kafka.interceptor.CountingProducerInterceptor.REPORTING_WINDOW_SIZE_MS;
import static course.kafka.model.TimestampedTemperatureReading.NORMAL_SENSOR_IDS;

@Slf4j
public class TimestampedTemperatureReadingsProducer implements Callable<String> {
    private static final String BASE_TRANSACTION_ID = "temperature-sensor-transaction-";
    public static final String INTERNAL_TEMP_TOPIC = "temperature";
    public static final String EXTERNAL_TEMP_TOPIC = "external-temperature";
    public static final String CLIENT_ID = "TemperatureReadingsProducerBySensorId";
    public static final String BOOTSTRAP_SERVERS = "localhost:9093";
    public static final int PRODUCER_TIMEOUT_SEC = 120;
    private final String sensorId;
    private long delayMs = 1000;
    private int numReadings = 10;
    private ExecutorService executor;
    private final String transactionId;
    private final String topic;


    public TimestampedTemperatureReadingsProducer(String transactionId, String sensorId, long delayMs,int numReadings, String topic) {
        this.sensorId = sensorId;
        this.delayMs = delayMs;
        this.numReadings = numReadings;
        this.transactionId = transactionId;
        this.topic = topic;
    }

    private static Producer<String, TimestampedTemperatureReading> createProducer(String transactionId) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024);
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, TemperatureReadingsPartitionerBySensorId.class.getName());
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, CountingProducerInterceptor.class.getName());
        props.put(REPORTING_WINDOW_SIZE_MS, 3000);
//        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionId);

        return new KafkaProducer<>(props);
    }

    @Override
    public String call() {
        var latch = new CountDownLatch(numReadings);
        Future<String> reporterFuture = null;
        try (var producer = createProducer(transactionId)) {
//            reporterFuture = executor.submit(new ProducerMetricReporter(producer));
//            producer.initTransactions();
            try {
//                producer.beginTransaction();
                var recordFutures = Flux.interval(Duration.ofMillis(delayMs))
                        .take(numReadings)
                        .map(n -> topic.equals(INTERNAL_TEMP_TOPIC)? 25 + Math.random() * 20 : 5 + Math.random() * 25)
                        .startWith(-20.0)
                        .map(t -> {
                            var reading = new TimestampedTemperatureReading(sensorId, t);
                            var record = new ProducerRecord(topic, reading.getSensorId(), reading);
                            return producer.send(record, (metadata, exception) -> {
                                if (exception != null) {
                                    log.error("Error sending temperature readings", exception);
                                }
                                log.info("SENSOR_ID: {}, MESSAGE: {}, TEMP: {}, Topic: {}, Partition: {}, Offset: {}, Timestamp: {}",
                                        sensorId, reading.getValue(),
                                        metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
                                latch.countDown();
                            });
                        }).collect(Collectors.toList()).block();

                latch.await(PRODUCER_TIMEOUT_SEC, TimeUnit.SECONDS);
                log.info("Transaction [{}] commited successfully", transactionId);
//                producer.commitTransaction();
            } catch (KafkaException kex) {
                log.error("Transaction [" + transactionId + "] was unsuccessful: ", kex);
                producer.abortTransaction();
            }
            log.info("!!! Closing producer for '{}'", sensorId);
        } catch (InterruptedException | ProducerFencedException | OutOfOrderSequenceException |
                 AuthorizationException e) {
            throw new RuntimeException(e);
        }
        return sensorId;
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // add new metrics:
        Map<String, String> metricTags = new LinkedHashMap<String, String>();
        metricTags.put("client-id", CLIENT_ID);
        metricTags.put("topic", INTERNAL_TEMP_TOPIC);

        // start temperature producers
        final List<TimestampedTemperatureReadingsProducer> producers = new ArrayList<>();
        var executor = Executors.newCachedThreadPool();
        ExecutorCompletionService<String> ecs = new ExecutorCompletionService(executor);

        for (int i = 0; i < 12; i++) {
            var producer = new TimestampedTemperatureReadingsProducer(
                    BASE_TRANSACTION_ID + "INTERNAL-" + i, NORMAL_SENSOR_IDS.get(i), 500, 150, INTERNAL_TEMP_TOPIC);
            producers.add(producer);
            ecs.submit(producer);
        }
        for (int i = 0; i < 12; i++) {
            var producer = new TimestampedTemperatureReadingsProducer(
                    BASE_TRANSACTION_ID + "EXTERNAL-" + i, NORMAL_SENSOR_IDS.get(i), 500, 150, EXTERNAL_TEMP_TOPIC);
            producers.add(producer);
            ecs.submit(producer);
        }
        for (int i = 0; i < producers.size(); i++) {
            System.out.printf("!!!!!!!!!!!! Producer for sensor '%s' COMPLETED.%n", ecs.take().get());
        }
        executor.shutdownNow();
    }
}
