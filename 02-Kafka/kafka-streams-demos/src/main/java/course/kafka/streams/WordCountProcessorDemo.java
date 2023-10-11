package course.kafka.streams;

import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class WordCountProcessorDemo {
    public static void main(String[] args) {
        // 1) Configure stream
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once_v2");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // 2) Provide supplier for local state stores
        Map<String, String> changelogConfig = new HashMap();
        changelogConfig.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1");

        StoreBuilder<KeyValueStore<String, Long>> countStoreSupplier =
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore("inmemory-word-counts"),
                        Serdes.String(),
                        Serdes.Long())
                        .withLoggingEnabled(changelogConfig);


        // 3) Create stream builder
        final Topology topology = new Topology(); // Configure processors DAG
        topology.addSource("Source", "streams-input")
                .addProcessor("Process", WordCountProcessor::new, "Source")
                .addStateStore(countStoreSupplier, "Process")
                .addSink("Sink", "latest-word-counts", "Process");

        // 4) Describe stream topology - optional
        System.out.println(topology.describe());

        // 5) Create streams instance
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch Ctrl-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        // 6) Start streams and await termination
        try {
            streams.start();
            latch.await();
        } catch (InterruptedException e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
