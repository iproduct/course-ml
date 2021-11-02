package course.kafka;


import course.kafka.model.Customer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;

import java.util.Properties;
import java.util.concurrent.Future;

@Slf4j
public class SecureDemoProducer {
    private Properties props = new Properties();
    private Producer producer;

    public SecureDemoProducer() {
        props.put("bootstrap.servers", "localhost:8092");
        // security config
        props.put("security.protocol", "SSL");
        props.put("ssl.endpoint.identification.algorithm", "");
        props.put("ssl.truststore.location", "D:\\CourseKafka\\kafka_2.12-2.2.1\\client.truststore.jks");
        props.put("ssl.truststore.password", "changeit");
        props.put("ssl.truststore.type", "JKS");

        //other config
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "course.kafka.serialization.JsonSerializer");
//        props.put("acks", "all");
//        props.put("enable.idempotence", "true");
//        props.put("max.request.size", "160");
//        props.put("transactional.id", "event-producer-1");
        props.put("partitioner.class", "course.kafka.partitioner.SimplePartitioner");

        producer = new KafkaProducer<String, Customer>(props);
    }

    public void run() {
//        producer.initTransactions();
//        try {
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
////                    cust = new Customer(i, "ABC " + i + " Ltd. ", "12345678" + i, "Sofiafsdfsfsdfsdfsdfdsfsdfdsfdsfdsfdsfdsdsfdsfsdfdsfdsfdsfdsfdsfdsfdsdsdsds 100" + i);
////                }
                ProducerRecord<String, Customer> record =
                        new ProducerRecord<>("test2", "" + i, cust);
                Future<RecordMetadata> futureResult = producer.send(record,
                        (metadata, exception) -> {
                            if (exception != null) {
                                log.error("Error publishing record: ", exception);
//                                producer.abortTransaction();
                                return;
                            }
                            log.info("topic: {}, partition {}, offset {}, timestamp: {}",
                                    metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
                        });
            }
//            producer.commitTransaction();
//        } catch (ProducerFencedException | OutOfOrderSequenceException |
//                AuthorizationException ex) {
//            producer.close();
//        } catch (KafkaException ex1) {
//            log.error("Transaction unsuccessfull: ", ex1);
//            producer.abortTransaction();
//        }
        producer.close();
    }

    public static void main(String[] args) throws InterruptedException {
        SecureDemoProducer producer = new SecureDemoProducer();
        producer.run();
        Thread.sleep(5000);
    }
}
