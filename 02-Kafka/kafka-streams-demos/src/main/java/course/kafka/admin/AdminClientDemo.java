package course.kafka.admin;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.acl.*;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.streams.processor.To;
import org.rocksdb.OperationType;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.common.resource.PatternType.ANY;
import static org.apache.kafka.common.resource.PatternType.LITERAL;
import static org.apache.kafka.common.resource.ResourceType.TOPIC;

@Slf4j
public class AdminClientDemo {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:8093");
        // security config
//        props.put("security.protocol", "SSL");
        props.put("ssl.endpoint.identification.algorithm", "");
        props.put("ssl.truststore.location", "D:\\CourseKafka\\kafka_2.13-3.2.0\\client.truststore.jks");
        props.put("ssl.truststore.password", "changeit");
        props.put("ssl.truststore.type", "JKS");
//        props.put("ssl.truststore.certificates", "CARoot");
        props.put("ssl.enabled.protocols", "TLSv1.2,TLSv1.1,TLSv1");
        props.put("ssl.protocol", "TLSv1.2");

        // SASL PLAIN Authentication
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='admin' password='admin123';");
//        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='trayan' password='trayan123';");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");

        try (Admin admin = Admin.create(props)) {
            String topicName = "temperature2";
//            int numPartitions = 12;
            int numPartitions = 1;
            short replicationFactor = 1;
            // Delete compacted topic if exists
//            try {
//                admin.deleteTopics(Collections.singleton(topicName)).all().get();
//                log.info("Successfully deleted topic: {}", topicName);
//            } catch (ExecutionException e) {
//                log.error("Unable to delete topic: " + topicName, e);
//            }
//            // Check topic is deleted
//            while (true) {
//                log.info("Checking topic '{}' was successfully deleted.\nCURRENT TOPICS:", topicName);
//                var topicsResult = admin.listTopics();
//                var allTopics = topicsResult.listings().get();
//                allTopics.forEach(System.out::println);
//                System.out.println();
//                if (!topicsResult.names().get().contains(topicName))
//                    break;
//                Thread.sleep(1000);
//            }
//            // Create compacted topic
//            var result = admin.createTopics(
//                    Collections.singleton(new NewTopic(topicName, numPartitions, replicationFactor)
//                            .configs(Map.of(
////                                    TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT,
////                                    TopicConfig.DELETE_RETENTION_MS_CONFIG, "100",
////                                    TopicConfig.SEGMENT_MS_CONFIG, "100",
////                                    TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.01",
////                                    TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, "100"
//                    )))
//            );
//            var future = result.topicId(topicName);
//            var topicUuid = future.get();
//            log.info("Successfully created topic: {} [{}]", topicName, topicUuid);
//            // Check topic is created
//            while (true) {
//                log.info("Checking topic '{}' was successfully created.\nCURRENT TOPICS:", topicName);
//                var topicsResult = admin.listTopics();
//                var allTopics = topicsResult.listings().get();
//                allTopics.forEach(System.out::println);
//                System.out.println();
//                if (topicsResult.names().get().contains(topicName))
//                    break;
//                Thread.sleep(1000);
//            }
//            var cluster = admin.describeCluster();
//            log.info("CLUSTER DESCRIPTION:\nID: {}\nController: {}\nNodes: {}\n",
//                    cluster.clusterId().get(),
//                    cluster.controller().get(),
//                    cluster.nodes().get()
//            );
//            // Describe all topics
//            var topicNames = admin.listTopics().names().get();
//            admin.describeTopics(topicNames).topicNameValues()
//                    .forEach((topic, topicDescriptionKafkaFuture) -> {
//                        try {
//                            log.info("Topic [{}] -> {}", topic, topicDescriptionKafkaFuture.get());
//                        } catch (InterruptedException | ExecutionException e) {
//                            throw new RuntimeException(e);
//                        }
//                    });
            // List InternalTemperatureEventsConsumer group offsets
//            admin.listConsumerGroupOffsets("InternalTemperatureEventsConsumer").partitionsToOffsetAndMetadata().get()
//                    .forEach((topicPartition, offsetAndMetadata) -> log.info("!!! InternalTemperatureEventsConsumer: Topic [{}] -> {}", topicPartition, offsetAndMetadata));
            // Set ACL for topic 'temperature2'
            admin.createAcls(Collections.singleton(
                            new AclBinding(new ResourcePattern(TOPIC, "temperature2", LITERAL),
                                    new AccessControlEntry("User:trayan", "*", AclOperation.WRITE, AclPermissionType.DENY))))
                    .values().forEach((aclBinding, voidKafkaFuture) -> log.info(">>>ACL Entry: {}", aclBinding));
//            admin.createAcls(Collections.singleton(
//                            new AclBinding(new ResourcePattern(TOPIC, "temperature2", LITERAL),
//                                    new AccessControlEntry("User:trayan", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW))))
//                    .values().forEach((aclBinding, voidKafkaFuture) -> log.info(">>>ACL Entry: {}", aclBinding));
//            admin.createAcls(Collections.singleton(
//                            new AclBinding(new ResourcePattern(TOPIC, "temperature2", LITERAL),
//                                    new AccessControlEntry("User:trayan", "*", AclOperation.ALL, AclPermissionType.ALLOW))))
//                    .values().forEach((aclBinding, voidKafkaFuture) -> log.info(">>>ACL Entry: {}", aclBinding));
//            admin.createAcls(Collections.singleton(
//                            new AclBinding(new ResourcePattern(TOPIC, "temperature2", LITERAL),
//                                    new AccessControlEntry("User:admin", "*", AclOperation.ALL, AclPermissionType.ALLOW))))
//                    .values().forEach((aclBinding, voidKafkaFuture) -> log.info(">>>ACL Entry: {}", aclBinding));
//            admin.deleteAcls(Collections.singleton(new AclBindingFilter(
//                    new ResourcePatternFilter(TOPIC,"temperature2", ANY),
//                    new AccessControlEntryFilter("trayan", null,  AclOperation.ANY, AclPermissionType.ANY))))
//                    .values().forEach((aclBindingFilter, filterResultsKafkaFuture) -> {
//                        try {
//                            log.info(">>>>> DELETED ACL Entry: {}", filterResultsKafkaFuture.get());
//                        } catch (InterruptedException | ExecutionException e) {
//                            throw new RuntimeException(e);
//                        }
//                    });
//            System.out.println("-------------------------------------------------------------------------------------------------");
            admin.describeAcls(
                    new AclBindingFilter(new ResourcePatternFilter(TOPIC,"temperature2", ANY),
                    new AccessControlEntryFilter(null, null,  AclOperation.ANY, AclPermissionType.ANY)))
                    .values().get().forEach((aclBinding) -> log.info("|||  ACL Entry: {}", aclBinding));
        }
        log.info("Demo complete.");
    }
}
