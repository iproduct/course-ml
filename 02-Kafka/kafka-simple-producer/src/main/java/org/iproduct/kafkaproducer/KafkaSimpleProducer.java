package org.iproduct.kafkaproducer;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Collections;

@SpringBootApplication
public class KafkaSimpleProducer {

    public static void main(String[] args) throws InterruptedException {
//        SpringApplication.run(SimpleKafkaProducer.class, args);
        SpringApplication app = new SpringApplication(KafkaSimpleProducer.class);
        app.setDefaultProperties(Collections.singletonMap("server.port", "8083"));
        app.run(args);
    }

//    @Bean
//    public NewTopic topic() {
//        return TopicBuilder.name("topic1")
//                .partitions(10)
//                .replicas(1)
//                .build();
//    }

    @Bean
    public ApplicationRunner runner(KafkaTemplate<String, String> template) {
        return args -> {
            for(int i = 0; i < 10; i++) {
                template.send("streamingTopic1", "key1", "abcdefgh");
                Thread.sleep(200);
            }
        };
    }

}
