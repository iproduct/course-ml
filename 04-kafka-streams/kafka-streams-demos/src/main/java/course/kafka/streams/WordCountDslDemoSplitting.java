package course.kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class WordCountDslDemoSplitting {
    public static void main(String[] args) {
        // 1) Configure stream
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once_v2");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // 2) Create stream builder
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream("streams-input");
//        stream.flatMap((k, sentence) ->
//                        Arrays.stream(sentence.toLowerCase(Locale.getDefault()).split("\\W+"))
//                                .map(w -> new KeyValue<String, String>(k, w))
//                                .collect(Collectors.toList())
        var wordsStream = stream.flatMapValues(sentence ->
                        Arrays.asList(sentence.toLowerCase(Locale.getDefault()).split("\\W+")))
                .selectKey((key, value) -> value);
//                .repartition(Repartitioned.as("word-counts-store").numberOfPartitions(4))

        var wordStreamBranches =
                wordsStream.split(Named.as("Branch-"))
                        .branch((key, value) -> key.startsWith("a"),  /* first predicate  */
                                Branched.as("A"))
                        .branch((key, value) -> key.startsWith("b"),  /* second predicate */
                                Branched.as("B"))
                        .defaultBranch(Branched.as("C"));            /* default branch */

        wordStreamBranches.get("Branch-A")
                .mapValues((key, value) -> key.length())
                .groupBy((key, value) -> key.charAt(0) + "", Grouped.valueSerde(Serdes.Integer()))
                .reduce((aggValue, newValue) -> aggValue + newValue /* adder */, Materialized.as("word-lengths-store"))
                .toStream()
                .mapValues((key, value) -> String.format("%-15s-> Sum:%4d", key, value.intValue()))
                .to("latest-latest-word-counts-a");

        wordStreamBranches.get("Branch-B")
                .mapValues((key, value) -> String.format("%-15s", key))
                .to("latest-latest-word-counts-b");

        wordStreamBranches.get("Branch-C")
                .mapValues((key, value) -> String.format("%-15s", key))
                .to("latest-latest-word-counts-c");


        // 3) Build stream topology
        final Topology topology = builder.build(); // build DAG
        System.out.println(topology.describe());

        // 4) Create streams instance
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

        // 5) Start streams and await termination
        try {
            streams.start();
            latch.await();
        } catch (InterruptedException e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
