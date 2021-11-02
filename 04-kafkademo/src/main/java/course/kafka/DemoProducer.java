package course.kafka;


import course.kafka.model.Customer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RecordTooLargeException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

@Slf4j
public class DemoProducer {
    private Properties kafkaProps = new Properties();
    private Producer producer;

    public DemoProducer() {
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "course.kafka.serialization.JsonSerializer");
        kafkaProps.put("acks", "all");
        kafkaProps.put("enable.idempotence", "true");
        kafkaProps.put("max.request.size", "160");
        kafkaProps.put("transactional.id", "event-producer-2");
        kafkaProps.put("partitioner.class", "course.kafka.partitioner.SimplePartitioner");
        producer = new KafkaProducer<String, Customer>(kafkaProps);
    }

    public void run() {
//        producer.initTransactions();
        try {
//            producer.beginTransaction();
            for (int i = 0; i < 10; i++) {

//                Map<String,String> customerData = new HashMap<>();
//                customerData.put("customerId", "" + i);
//                customerData.put("name", "customer-" + i);
//                customerData.put("address", "address-" + i);
//                int[] eik = new int[9];
//                Arrays.fill(eik, i);
//                System.out.println( Arrays.stream(eik).mapToObj(n -> "" + n)
//                    .collect(Collectors.joining("")).toString());
                Customer cust = new Customer(i, "ABC " + i + " Ltd. ", "12345678" + i, "Sofia 100" + i);
//                if (i == 9) {
//                    cust = new Customer(i, "ABC " + i + " Ltd. ", "12345678" + i, "Sofiafsdfsfsdfsdfsdfdsfsdfdsfdsfdsfdsfdsdsfdsfsdfdsfdsfdsfdsfdsfdsfdsdsdsds 100" + i);
//                }
                ProducerRecord<String, Customer> record =
                        new ProducerRecord<>("events", "" + i, cust);
                Future<RecordMetadata> futureResult = producer.send(record,
                        (metadata, exception) -> {
                            if (exception != null) {
                                log.error("Error publishing record: ", exception);
                                producer.abortTransaction();
                                return;
                            }
                            log.info("topic: {}, partition {}, offset {}, timestamp: {}",
                                    metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
                        });
            }
//            producer.commitTransaction();
        } catch (ProducerFencedException | OutOfOrderSequenceException |
                AuthorizationException ex) {
            producer.close();
        } catch (KafkaException ex1) {
            log.error("Transaction unsuccessfull: ", ex1);
            producer.abortTransaction();
        }
        producer.close();
    }

    public static void main(String[] args) throws InterruptedException {
        DemoProducer producer = new DemoProducer();
        producer.run();
        Thread.sleep(5000);
    }
}
