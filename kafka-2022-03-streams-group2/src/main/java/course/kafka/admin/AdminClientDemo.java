package course.kafka.admin;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.TopicConfig;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

@Slf4j
public class AdminClientDemo {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");

        try (Admin admin = Admin.create(props)) {
            String topicName = "my-compacted-topic";
            int partitions = 12;
            short replicationFactor = 1;
            // Create a compacted topic
            try {
                admin.deleteTopics(Collections.singleton(topicName)).all().get();
                log.info("Topic '{}' deleted successfully", topicName);
            } catch (ExecutionException | InterruptedException e) {
                log.error("Error deleting topic: " + topicName, e);
            }
            // wait until topic is deleted
            while (true) {
                log.info("Making sure the topic deletion completed.");
                Set<String> listedTopics = admin.listTopics().names().get();
                log.info("Current list of topics: {}", listedTopics);
                if (!listedTopics.contains(topicName)) {
                    break;
                }
                Thread.sleep(1000);
            }

            CreateTopicsResult result = admin.createTopics(Collections.singleton(
                    new NewTopic(topicName, partitions, replicationFactor)
                            .configs(Map.of(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT,
                                    TopicConfig.DELETE_RETENTION_MS_CONFIG, "100",
                                    TopicConfig.SEGMENT_MS_CONFIG, "100",
                                    TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.01",
                                    TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, "100"
                            ))));

            // Call values() to get the result for a specific topic
            KafkaFuture<Uuid> future = result.topicId(topicName);

            // Call get() to block until the topic creation is complete or has failed
            // if creation failed the ExecutionException wraps the underlying cause.
            var uuid = future.get();
//                if (ex != null) {
//                    log.error("Error creating topic: " + topicName, ex);
//                }
            log.info("Topic UUID: '{}'", uuid.toString());
            // wait until topic is created
            while (true) {
                log.info("Making sure the topic creation completed.");
                Set<String> listedTopics = admin.listTopics().names().get();
                log.info("Current list of topics AFTER create: {}", listedTopics);
                if (listedTopics.contains(topicName)) {
                    break;
                }
                Thread.sleep(1000);
            }
            var descr = admin.describeTopics(Collections.singleton(topicName)).allTopicNames().get();
//            log.info("!!!!! Description: {}", descr.toString());
            descr.forEach((topic, topicDescription) -> {
                log.info("!!! Topic '{}' -> {}", topic, topicDescription);
            });

        }
    }
}
