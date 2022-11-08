package course.kafka.producer;

import ch.qos.logback.core.util.ExecutorServiceUtil;
import course.kafka.interceptor.CountingProducerInterceptor;
import course.kafka.metrics.ProducerMetricReporter;
import course.kafka.model.TemperatureReading;
import course.kafka.partitioner.TemperatureReadingsPartitioner;
import course.kafka.serialization.JsonSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Min;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static course.kafka.interceptor.CountingProducerInterceptor.REPORTING_WINDOW_SIZE_MS;
import static course.kafka.model.TemperatureReading.HF_SENSOR_IDS;
import static course.kafka.model.TemperatureReading.NORMAL_SENSOR_IDS;

@Slf4j
public class SimpleTemperatureReadingsProducer implements Callable<String> {
    private static final String BASE_TRANSACTION_ID = "temperature-sensor-transaction-";
    public static final String TOPIC = "temperature";
    public static final String CLIENT_ID = "TemperatureReadingsProducer";
    public static final String BOOTSTRAP_SERVERS = "localhost:9093";
    public static final String HIGH_FREQUENCY_SENSORS = "sensors.highfrequency";
    public static final int PRODUCER_TIMEOUT_SEC = 20;
    public static String MY_MESSAGE_SIZE_SENSOR = "my-message-size";
    private String sensorId;
    private long maxDelayMs = 10000;
    private int numReadings = 10;
    private ExecutorService executor;
    private String transactionId;


    public SimpleTemperatureReadingsProducer(String transactionId, String sensorId, long maxDelayMs, int numReadings, ExecutorService executor) {
        this.sensorId = sensorId;
        this.maxDelayMs = maxDelayMs;
        this.numReadings = numReadings;
        this.executor = executor;
        this.transactionId = transactionId;
    }

    private static Producer<String, TemperatureReading> createProducer(String transactionId) {
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
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, TemperatureReadingsPartitioner.class.getName());
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, CountingProducerInterceptor.class.getName());
        props.put(REPORTING_WINDOW_SIZE_MS, 3000);
        props.put(HIGH_FREQUENCY_SENSORS, HF_SENSOR_IDS.stream().collect(Collectors.joining(",")));
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionId);

        return new KafkaProducer<>(props);
    }

    @Override
    public String call() {
        // get metrics
//        Map<String, String> metricTags = new LinkedHashMap<String, String>();
//        metricTags.put("client-id", CLIENT_ID);
//        metricTags.put("topic", TOPIC);
//        MetricConfig metricConfig = new MetricConfig().tags(metricTags);
//        Metrics metrics = new Metrics(metricConfig);
//        Sensor sensor = metrics.sensor(MY_MESSAGE_SIZE_SENSOR);

        var latch = new CountDownLatch(numReadings);
        Future<String> reporterFuture = null;
        try (var producer = createProducer(transactionId)) {
//            reporterFuture = executor.submit(new ProducerMetricReporter(producer));
            producer.initTransactions();
            var i = new AtomicInteger();
            try {
                producer.beginTransaction();
                var recordFutures = new Random().doubles(numReadings).map(t -> t * 40)
                        .peek(t -> {
                            try {
                                Thread.sleep((int) (Math.random() * maxDelayMs));
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            var ival = i.incrementAndGet();
//                            if(ival == 3){
//                                throw new KafkaException("Invalid temperature reading");
//                            }
                        })
                        .mapToObj(t -> new TemperatureReading(UUID.randomUUID().toString(), sensorId, t))
                        .map(reading -> new ProducerRecord(TOPIC, reading.getId(), reading))
                        .map(record -> {
                            return producer.send(record, (metadata, exception) -> {
                                if (exception != null) {
                                    log.error("Error sending temperature readings", exception);
                                }
                                // as messages are sent we record the sizes
//                                sensor.record(metadata.serializedValueSize());
                                log.info("SENSOR_ID: {}, MESSAGE: {}, Topic: {}, Partition: {}, Offset: {}, Timestamp: {}",
                                        sensorId, i.get(),
                                        metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
                                latch.countDown();
                            });
                        }).collect(Collectors.toList());

                latch.await(15, TimeUnit.SECONDS);
                log.info("Transaction [{}] commited successfully", transactionId);
                producer.commitTransaction();
            } catch (KafkaException kex) {
                log.error("Transaction [" + transactionId + "] was unsuccessful: ", kex);
                producer.abortTransaction();
            }
            log.info("!!! Closing producer for '{}'", sensorId);
        } catch (InterruptedException | ProducerFencedException | OutOfOrderSequenceException |
                 AuthorizationException e) {
            throw new RuntimeException(e);
        }
//        finally { // Cancel metrics reporter thread
//            if (reporterFuture != null) {
//                log.info("Canceled the metrics reporter for [{}]: {}", sensorId, reporterFuture.cancel(true));
//            }
//        }
        return sensorId;
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // add new metrics:
        Map<String, String> metricTags = new LinkedHashMap<String, String>();
        metricTags.put("client-id", CLIENT_ID);
        metricTags.put("topic", TOPIC);

        MetricConfig metricConfig = new MetricConfig().tags(metricTags);
        Metrics metrics = new Metrics(metricConfig); // this is the global repository of metrics and sensors

        Sensor sensor = metrics.sensor(MY_MESSAGE_SIZE_SENSOR);

        MetricName metricName = metrics.metricName(MY_MESSAGE_SIZE_SENSOR + "-avg", "producer-metrics", "my message average size");
        sensor.add(metricName, new Avg());

        metricName = metrics.metricName(MY_MESSAGE_SIZE_SENSOR + "-max", "producer-metrics", "my message max size");
        sensor.add(metricName, new Max());

        metricName = metrics.metricName(MY_MESSAGE_SIZE_SENSOR + "-min", "producer-metrics", "my message max size", "client-id", CLIENT_ID, "topic", TOPIC);
        sensor.add(metricName, new Min());

        // start temperature producers
        final List<SimpleTemperatureReadingsProducer> producers = new ArrayList<>();
        var executor = Executors.newCachedThreadPool();
        ExecutorCompletionService<String> ecs = new ExecutorCompletionService(executor);
//        for (int i = 0; i < HF_SENSOR_IDS.size(); i++) {
//            var producer = new SimpleTemperatureReadingsProducer(
//                    BASE_TRANSACTION_ID + "HF-" + i, HF_SENSOR_IDS.get(i), 250, 240, executor);
//            producers.add(producer);
//            ecs.submit(producer);
//        }
//        for (int i = 0; i < NORMAL_SENSOR_IDS.size(); i++) {
        for (int i = 0; i < 1; i++) {
            var producer = new SimpleTemperatureReadingsProducer(
                    BASE_TRANSACTION_ID + "LF-" + i, NORMAL_SENSOR_IDS.get(i), 10, 3, executor);
            producers.add(producer);
            ecs.submit(producer);
        }
        for (int i = 0; i < producers.size(); i++) {
            System.out.printf("!!!!!!!!!!!! Producer for sensor '%s' COMPLETED.%n", ecs.take().get());
        }
        executor.shutdownNow();
    }
}
