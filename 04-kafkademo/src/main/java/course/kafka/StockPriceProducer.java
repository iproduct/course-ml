package course.kafka;


import course.kafka.model.Customer;
import course.kafka.model.StockPrice;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

@Slf4j
public class StockPriceProducer {
    private Properties kafkaProps = new Properties();
    private Producer producer;
    List<StockPrice> stocks = Arrays.asList(
            new StockPrice("VMW", "VMWare", 215.35),
            new StockPrice("GOOG", "Google", 309.17),
            new StockPrice("CTXS", "Citrix Systems, Inc.", 112.11),
            new StockPrice("DELL", "Dell Inc.", 92.93),
            new StockPrice("MSFT", "Microsoft", 255.19),
            new StockPrice("ORCL", "Oracle", 115.72),
            new StockPrice("RHT", "Red Hat", 111.27)
    );

    public StockPriceProducer() {
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "course.kafka.serialization.JsonSerializer");
        kafkaProps.put("acks", "all");
        kafkaProps.put("partitioner.class", "course.kafka.partitioner.SimplePartitioner");
        producer = new KafkaProducer<String, StockPrice>(kafkaProps);
    }

    public void run() {
        stocks.stream().forEach(stock -> {
            ProducerRecord<String, StockPrice> record =
                new ProducerRecord<>("prices", stock.getSymbol(), stock);
            Future<RecordMetadata> futureResult = producer.send(record,
                (metadata, exception) -> {
                    if (exception != null) {
                        log.error("Error publishing record: ", exception);
                        return;
                    }
                    log.info("topic: {}, partition {}, offset {}, timestamp: {}",
                        metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
                });
        });

        producer.close();
    }

    public static void main(String[] args) throws InterruptedException {
        StockPriceProducer producer = new StockPriceProducer();
        producer.run();
        Thread.sleep(5000);
    }
}
