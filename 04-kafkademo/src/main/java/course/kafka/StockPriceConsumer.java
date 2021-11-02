package course.kafka;

import course.kafka.dao.PricesDAO;
import course.kafka.model.StockPrice;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.json.simple.JSONObject;

import java.sql.SQLException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static course.kafka.StockPriceConstants.GROUP_ID_PROP;
import static course.kafka.StockPriceConstants.PRICES_TOPIC;

@Slf4j
public class StockPriceConsumer implements Runnable{
    public static final String GROUP_ID = "stock-price-consumer";
    private Properties props = new Properties();
    private KafkaConsumer<String, StockPrice> consumer;
    private Map<String, Integer> eventMap = new ConcurrentHashMap<>();
    private Map<TopicPartition, OffsetAndMetadata> currentOffsets =
            new HashMap<>();
    private int count = 0;
    private PricesDAO dao;

    public StockPriceConsumer(int consumerId) throws SQLException {
        log.info("Creating consumer with ID:{}" + GROUP_ID + "-" + consumerId);
        props.put("bootstrap.servers", "localhost:9092");
        props.put("client.id", GROUP_ID + "-" + consumerId);
        props.put(GROUP_ID_PROP, GROUP_ID);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "course.kafka.serialization.JsonDeserializer");
        props.put("value.deserializer.class", "course.kafka.model.StockPrice");
        props.put("enable.auto.commit", "false");
        consumer = new KafkaConsumer<>(props);
        dao = new PricesDAO();
        dao.init();

        // add shutdown hook
        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Starting exit ...");
            consumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                log.error("Thread join interrupted.");
            }
        }));



    }

    public void run() {
        consumer.subscribe(Collections.singletonList(PRICES_TOPIC),
                new StockPriceRebalanceListener(consumer, dao, GROUP_ID));
        try {
            while (!Thread.currentThread().isInterrupted()) {
                ConsumerRecords<String, StockPrice> records = consumer.poll(Duration.ofMillis(1000));
                if (records.count() > 0) {
                    log.info("Fetched {} records:", records.count());
                    for (ConsumerRecord<String, StockPrice> rec : records) {
                        log.debug("Record - topic: {}, partition: {}, offset: {}, timestamp: {}, value: {}.",
                                rec.topic(),
                                rec.partition(),
                                rec.offset(),
                                rec.timestamp(),
                                rec.value());
                        log.info("{} -> {}, {}, {}, {}", rec.key(),
                                rec.value().getId(),
                                rec.value().getSymbol(),
                                rec.value().getName(),
                                rec.value().getPrice());
                        int updatedCount = 1;
                        if (eventMap.containsKey(rec.key())) {
                            updatedCount = eventMap.get(rec.key()) + 1;
                        }
                        eventMap.put(rec.key(), updatedCount);

                        //update currentOffsets
                        currentOffsets.put(new TopicPartition(rec.topic(), rec.partition()),
                                new OffsetAndMetadata(rec.offset() + 1));

                        //persist data to DB
                        dao.insertPrice(rec.value());

//                        if(++count % 3 == 0) {
//                            commitOffsets();
//                        }
                    }
                    // commit batch + offsets in single transaction = exactly once semantics
                    dao.updateOffsets(GROUP_ID, currentOffsets);
                    try {
                        dao.commitTransaction();
                    } catch (SQLException e) {
                        log.error("Error commiting transaction.", e);
                        dao.rollbackTransaction();
                    }

                    JSONObject json = new JSONObject(eventMap);
                    log.info(json.toJSONString());
                }

            }
        } catch (WakeupException | InterruptException e) {
            log.info("Application tiring down...");
        } catch (Exception e) {
            log.error("Error polling data.");
        } finally {
//            try {
//                consumer.commitSync();
//            } finally {
                consumer.close();
                log.info("Consumer closed and we are down.");
//            }
        }
    }

    private void commitOffsets() {
        consumer.commitAsync(currentOffsets, (offsets, exception) -> {
            if (exception != null) {
                log.error("Error commiting offsets: {}, exception: {}", offsets, exception);
                return;
            }
            log.debug("Offsets commited: {}", offsets);
        });
    }

    public static void main(String[] args) throws SQLException {
        StockPriceConsumer demoConsumer = new StockPriceConsumer(0);
        demoConsumer.run();
    }
}
