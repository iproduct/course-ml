package course.dml.producer;

import course.dml.model.StockPrice;
import course.dml.serde.JsonSerializer;
import course.dml.service.StockPricesGenerator;
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;

@Slf4j
public class StockPricesProducer implements Callable<String> {
    public static final String TOPIC = "prices";
    public static final String CLIENT_ID = "StockPricesProducer";
    public static final String BOOTSTRAP_SERVERS = "localhost:9093";
    public static final long MAX_DEMO_TIME_MS = 120_000;
    public static final long NUM_PRODUCERS = 1;

    private long producerId = 1L;
    private long maxDelayMs = 10000;
    private long numReadings = 10;
    private ExecutorService executor;

    public StockPricesProducer(long producerId, long maxDelayMs, long numReadings) {
        this.producerId = producerId;
        this.maxDelayMs = maxDelayMs;
        this.numReadings = numReadings;
    }

    private static Producer<String, StockPrice> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024);
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000);

        return new KafkaProducer<String, StockPrice>(props);
    }

    @Override
    public String call() throws Exception {
        var latch = new CountDownLatch(1);
        try(var producer = createProducer()) {
            try {
                StockPricesGenerator.getQuotesStream(numReadings, Duration.ofMillis(maxDelayMs))
                        .map(quote -> new ProducerRecord<>(TOPIC, quote.getSymbol(), quote))
                        .subscribe(
                                record -> producer.send(record, (metadata, exception) -> {
                                    if (exception != null) {
                                        log.error("Error sending price quotation", exception);
                                    }
                                    log.info("SYMBOL: {}, ID: {}, PRICE: {}, TOPIC: {}, PARTITION: {}, OFFSET: {}, TIMESTAMP: {}",
                                            record.key(), record.value().getId(), record.value().getPrice(),
                                            metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
                                }),
                                err -> log.error("Error processing quotes stream", err),
                                () -> {
                                    log.info("Quotes stream completed successfully for client: {}-{}",
                                            CLIENT_ID, producerId);
                                    latch.countDown();
                                }
                        );
                latch.await(MAX_DEMO_TIME_MS, TimeUnit.MILLISECONDS);
            } catch (KafkaException kex) {
                log.error("Producer " + producerId + " exception:", kex);
            }
            log.info("Closing producer: {}-{}", CLIENT_ID, producerId);
        } catch (InterruptedException | ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            throw new RuntimeException(e);
        }
        return "Producer " + producerId + " COMPLETE.";
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        // start quotes producers
        final List<StockPricesProducer> producers = new ArrayList<>();
        var executor = Executors.newCachedThreadPool();
        var ecs = new ExecutorCompletionService<String>(executor);
        for(int i = 0; i < NUM_PRODUCERS; i++) {
            var producer = new StockPricesProducer(i, 1000, 20);
            producers.add(producer);
            ecs.submit(producer);
        }
        for(int i = 0; i < NUM_PRODUCERS; i++) {
            System.out.printf("!!!!!! %s%n", ecs.take().get());
        }
        executor.shutdownNow();
    }
}
