package dml.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.function.BiFunction;

@Slf4j
public class DemoConsumer {
    private Properties props = new Properties();
    KafkaConsumer<String, String> consumer;

    public DemoConsumer() {
        props.setProperty("bootstrap.servers", "localhost:9093");
        props.setProperty("group.id", "dmlConsumer");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(props);

    }

    public void run() {
        consumer.subscribe(Collections.singletonList("events"));

//        TopicPartition partition = new TopicPartition("events", 0);
//        consumer.assign(List.of(partition));
//        consumer.seek(partition, 0);

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                if (records.count() > 0) {
                    for (ConsumerRecord<String, String> record : records) {
                        System.out.printf("offset = %d, key = %s, value = %s, headers = %s, topic = %s, partition = %d%n",
                                record.offset(), record.key(), record.value(), record.headers(), record.topic(), record.partition());
                    }
                }
            }
        } finally {
            consumer.close();
        }
    }

    public static void main(String[] args) {
        DemoConsumer consumer = new DemoConsumer();
        consumer.run();
    }
}
