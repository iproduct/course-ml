package course.dml.consumer;

import course.dml.model.StockPrice;
import course.dml.serde.JsonDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static course.dml.serde.JsonDeserializer.KEY_CLASS;
import static course.dml.serde.JsonDeserializer.VALUE_CLASS;
import static java.lang.Thread.interrupted;

@Slf4j
public class SimpleStockPricesConsumer implements Callable<Void> {
    public static final String TOPIC = "prices";
    public static final String CONSUMER_GROUP = "SimpleStockPricesConsumer";
    public static final String BOOTSTRAP_SERVERS = "localhost:9093";
    public static final long POLLING_DURATION_MS = 100;
    public static final long NUM_CONSUMERS = 1;

    private volatile boolean canceled = false;
    private KafkaConsumer<String, StockPrice> consumer;

    private static KafkaConsumer<String, StockPrice> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(KEY_CLASS, String.class.getName());
        props.put(VALUE_CLASS, StockPrice.class.getName());

        return new KafkaConsumer<>(props);
    }

    public void cancel() {
        canceled = true;
    }

    @Override
    public Void call() throws Exception {
        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
        try {
            consumer = createConsumer();
            consumer.subscribe(List.of(TOPIC));
            while (!canceled && !interrupted()) {
                var records = consumer.poll(Duration.ofMillis(POLLING_DURATION_MS));
                if (records.count() == 0) continue;
                for (var r : records) {
                    currentOffsets.compute(new TopicPartition(r.topic(), r.partition()), (key, oldV) ->
                            oldV == null || oldV.offset() < r.offset() + 1 ?
                                    new OffsetAndMetadata(r.offset() + 1, "no metadata") :
                                    oldV
                    );
                    log.info("!!!!!!!!!!!!!!!!!!!!!!!  -->  [TOPIC: {}, PARTITION: {}, OFFSET: {}, TIMESTAMP: {}, LEADER_EPOCH: {}]: {} -->     {}%n",
                            r.topic(), r.partition(), r.offset(), r.timestamp(), r.leaderEpoch(),
                            r.key(), r.value());
                }
                consumer.commitAsync(currentOffsets, ((offsets, exception) -> {
                    if (exception != null) {
                        log.error("Comsumer [" + consumer.groupMetadata().groupId() + "] failed to commit offsets: " + offsets, exception);
                    } else {
                        offsets.forEach((tp, offs) -> {
                            log.debug("Consumer [{}] successfully committed offsets: {}: {}",
                                    consumer.groupMetadata().groupId(), tp.toString(), offs.toString());
                        });
                    }
                }));
            }
        } catch (WakeupException wex) {
            log.warn("Consumer [" + consumer.groupMetadata().groupId() + "] was INTERRUPTED.");
            if (!canceled) {
                throw wex;
            }
        } catch (Exception ex) {
            log.warn("Consumer [" + consumer.groupMetadata().groupId() + "] ERROR.");
            throw ex;
        } finally {
            consumer.close();
        }

        return null;
    }

    public void shutdown() {
        cancel();
        consumer.wakeup();
    }

    public static void main(String[] args) {
        final List<SimpleStockPricesConsumer> consumers = new ArrayList<>();
        final List<Future<Void>> consumerFutures = new ArrayList<>();
        var executor = Executors.newCachedThreadPool();
        for (int i = 0; i < NUM_CONSUMERS; i++) {
            var consumer = new SimpleStockPricesConsumer();
            consumers.add(consumer);
            consumerFutures.add(executor.submit(consumer));
        }
        System.out.println("Hit <ENTER> to close");
        new Scanner(System.in).nextLine();
        System.out.println("Closing consumers ...");
        consumers.forEach(SimpleStockPricesConsumer::shutdown);
        consumerFutures.forEach(f -> f.cancel(true));
        executor.shutdown();
    }
}
