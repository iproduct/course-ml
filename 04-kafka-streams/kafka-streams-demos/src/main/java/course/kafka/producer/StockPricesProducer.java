package course.kafka.producer;

import course.kafka.model.StockPrice;
import course.kafka.partitioner.StockPricePartitioner;
import course.kafka.serialization.JsonSerializer;
import course.kafka.service.StockPricesGenerator;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

import static course.kafka.model.TemperatureReading.NORMAL_SENSOR_IDS;

@Slf4j
public class StockPricesProducer implements Callable<String> {
    private static final String BASE_TRANSACTION_ID = "stock-prices-transaction-";
    public static final String TOPIC = "prices";
    public static final String CLIENT_ID = "StockPricesProducer";
    public static final String BOOTSTRAP_SERVERS = "localhost:9093";
    private long maxDelayMs = 10000;
    private int numReadings = 10;
    private ExecutorService executor;
    private String transactionId;


    public StockPricesProducer(String transactionId, long maxDelayMs, int numReadings, ExecutorService executor) {
        this.maxDelayMs = maxDelayMs;
        this.numReadings = numReadings;
        this.executor = executor;
        this.transactionId = transactionId;
    }

    private static Producer<String, StockPrice> createProducer(String transactionId) {
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
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, StockPricePartitioner.class.getName());
//        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, CountingProducerInterceptor.class.getName());
//        props.put(REPORTING_WINDOW_SIZE_MS, 3000);
//        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionId);

        return new KafkaProducer<>(props);
    }

    @Override
    public String call() {
        var latch = new CountDownLatch(1);
        try (var producer = createProducer(transactionId)) {
            try {
                StockPricesGenerator.getQuotesStream(numReadings, Duration.ofMillis(maxDelayMs))
                        .map(quote -> new ProducerRecord<>(TOPIC, quote.getSymbol(), quote))
                        .subscribe(record -> {
                                    producer.send(record, (metadata, exception) -> {
                                        if (exception != null) {
                                            log.error("Error sending temperature readings", exception);
                                        }
                                        log.info("SYMBOL: {}, ID: {}, Topic: {}, Partition: {}, Offset: {}, Timestamp: {}",
                                                record.key(), record.value().getId(),
                                                metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
                                    });
                                }, error -> {
                                    log.error("Error processing stock prices stream:", error);
                                    latch.countDown();
                                },
                                () -> {
                                    log.info("Stock prices stream completed successfully.");
                                    latch.countDown();
                                });

                latch.await(200, TimeUnit.SECONDS);
                log.info("Transaction [{}] commited successfully", transactionId);
            } catch (KafkaException kex) {
                log.error("Producer [" + transactionId + "] was unsuccessful: ", kex);
            }
            log.info("!!! Closing producer with transactionID '{}'", transactionId);
        } catch (InterruptedException | ProducerFencedException | OutOfOrderSequenceException |
                 AuthorizationException e) {
            throw new RuntimeException(e);
        }
        return transactionId;
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // add new metrics:
        Map<String, String> metricTags = new LinkedHashMap<String, String>();
        metricTags.put("client-id", CLIENT_ID);
        metricTags.put("topic", TOPIC);

        MetricConfig metricConfig = new MetricConfig().tags(metricTags);
        Metrics metrics = new Metrics(metricConfig); // this is the global repository of metrics and sensors

        // start temperature producers
        final List<StockPricesProducer> producers = new ArrayList<>();
        var executor = Executors.newCachedThreadPool();
        ExecutorCompletionService<String> ecs = new ExecutorCompletionService(executor);
        for (int i = 0; i < 1; i++) {
            var producer = new StockPricesProducer(
                    "Stock-Producer-" + 0, 1000, 1400, executor);
            producers.add(producer);
            ecs.submit(producer);
        }
        for (int i = 0; i < producers.size(); i++) {
            System.out.printf("!!!!!!!!!!!! Producer for sensor '%s' COMPLETED.%n", ecs.take().get());
        }
        executor.shutdownNow();
    }
}
