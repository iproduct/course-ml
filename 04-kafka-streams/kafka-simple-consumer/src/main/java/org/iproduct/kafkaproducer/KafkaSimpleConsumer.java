package org.iproduct.kafkaproducer;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.kstream.Windowed;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;

@SpringBootApplication
public class KafkaSimpleConsumer {

    public static void main(String[] args) {
        SpringApplication.run(KafkaSimpleConsumer.class, args);
    }

//    @Bean
//    public NewTopic topic() {
//        return TopicBuilder.name("topic1")
//                .partitions(10)
//                .replicas(1)
//                .build();
//    }

//    @KafkaListener(id = "myId", topics = "streamingTopic2")
    @KafkaListener(id = "myId", topics = "minSweepDistance", batch = "false", properties = "max.poll.records=1")
    public void listen(ConsumerRecord<Integer, String> inRecord) {
        System.out.printf("Robot %d -> %s%n", inRecord.key(), inRecord.value());
    }
}



