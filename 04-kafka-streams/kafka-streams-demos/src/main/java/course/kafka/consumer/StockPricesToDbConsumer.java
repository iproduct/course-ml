package course.kafka.consumer;

import course.kafka.dao.PricesDAO;
import course.kafka.listener.StockPriceRebalanceListener;
import course.kafka.model.StockPrice;
import course.kafka.serialization.JsonDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.Thread.interrupted;

@Slf4j
public class StockPricesToDbConsumer implements Runnable {
    public static final String TOPIC = "prices";
    public static final String CONSUMER_GROUP = "StockPricesToDbConsumer";
    public static final String BOOTSTRAP_SERVERS = "localhost:9093";//,localhost:9094,localhost:9095";
    public static final String KEY_CLASS = "key.class";
    public static final String VALUE_CLASS = "values.class";
    public static final long POLLING_DURATION_MS = 100;

    private volatile boolean canceled;

    private PricesDAO dao;

    private KafkaConsumer<String,StockPrice> consumer;

    public StockPricesToDbConsumer(PricesDAO dao) {
        this.dao = dao;
    }

    private static KafkaConsumer<String, StockPrice> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
//        props.put(ConsumerConfig.DEFAULT_ISOLATION_LEVEL, IsolationLevel.READ_COMMITTED);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.toString().toLowerCase());
        props.put(KEY_CLASS, String.class.getName());
        props.put(VALUE_CLASS, StockPrice.class.getName());

        return new KafkaConsumer<>(props);
    }

    public void cancel() {
        canceled = true;
    }


    @Override
    public void run() {
        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
        try {
            consumer = createConsumer();
            consumer.subscribe(List.of(TOPIC), new StockPriceRebalanceListener(consumer, dao, CONSUMER_GROUP));
            try {
                while (!canceled && !interrupted()) {
                    var records = consumer.poll(
                            Duration.ofMillis(POLLING_DURATION_MS));
                    if (records.count() == 0) continue;
                    try {
                        for (var r : records) {
                            dao.insertPrice(r.value());
                            currentOffsets.compute(new TopicPartition(r.topic(), r.partition()), (key, oldV) ->
                                    oldV == null || r.offset() + 1 > oldV.offset() ?
                                            new OffsetAndMetadata(r.offset() + 1, "no metadata")
                                            : oldV
                            );
                            log.info("[Topic: {}, Partition: {}, Offset: {}, Timestamp: {}, Leader Epoch: {}]: {} -->\n    {}",
                                    r.topic(), r.partition(), r.offset(), r.timestamp(), r.leaderEpoch(), r.key(), r.value());
                        }
                        dao.updateOffsets(CONSUMER_GROUP, currentOffsets);
                        dao.commitTransaction();
                        consumer.commitAsync(currentOffsets, (offsets, exception) -> {
                            if (exception != null) {
                                log.error("Consumer [" + consumer.groupMetadata().groupId() + "] FAILED to commit offsets: " + offsets, exception);
                            } else {
                                offsets.forEach((tp, offs) -> {
                                    log.debug("Consumer [{}] SUCCESSFULLY commited offsets: {} : {}", consumer.groupMetadata().groupId(), tp.toString(), offs.offset());
                                });
                            }
                        });
                    } catch (SQLException ex) {
                        try {
                            log.error("Consumer [" + consumer.groupMetadata().groupId() + "] FAILED - rollbacking database  transaction.", ex);
                            dao.rollbackTransaction();
                        } catch (SQLException e) {
                            log.error("Error rolbacking database transaction for consumer [" + consumer.groupMetadata().groupId() + "].", ex);
                        } finally {
                            throw new RuntimeException(ex);
                        }
                    } catch (WakeupException wex){
                        log.warn("Consumer [" + consumer.groupMetadata().groupId() + "] was INTERRUPTED.");
                        // Ignore exception if closing
                        if (!canceled) throw wex;
                    }
                }
            } finally {
                log.warn("Consumer [" + consumer.groupMetadata().groupId() + "] was canceled.");
            }
        } finally {
            consumer.close();
        }
    }

    // Shutdown hook which can be called from a separate thread
    public void shutdown() {
        cancel();
        consumer.wakeup();
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException, SQLException, IOException, ClassNotFoundException {
        var dao = new PricesDAO();
        dao.init();
        StockPricesToDbConsumer consumer = new StockPricesToDbConsumer(dao);
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

