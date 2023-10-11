package course.kafka.streams;

import course.kafka.model.DoubleStatistics;
import course.kafka.model.TimestampedTemperatureReading;
import course.kafka.serialization.JsonDeserializer;
import course.kafka.serialization.JsonSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static java.lang.Double.max;
import static java.lang.Double.min;
import static org.apache.kafka.streams.kstream.Consumed.with;


public class WindowedStatisticsSuppressionTemperatureReadings03 {
    public static final String INTERNAL_TEMP_TOPIC = "temperature";
    public static final String EXTERNAL_TEMP_TOPIC = "external-temperature";
    public static final String OUTPUT_TOPIC = "events";
    public static final long WINDOW_SIZE_MS = 5000;


    public static void main(String[] args) {
        // 1) Configure stream
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "heating-bills");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once_v2");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // create custom JSON Serdes
        Serde<TimestampedTemperatureReading> readingsJsonSerde = Serdes.serdeFrom(
                new JsonSerializer<>(), new JsonDeserializer<>(TimestampedTemperatureReading.class));
        Serde<DoubleStatistics> doubleStatisticsSerde = Serdes.serdeFrom(
                new JsonSerializer<>(), new JsonDeserializer<>(DoubleStatistics.class));

        // 2) Create stream builder
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, TimestampedTemperatureReading> internalTemperature = builder
                .stream(INTERNAL_TEMP_TOPIC, with(Serdes.String(), readingsJsonSerde));
        KStream<String, TimestampedTemperatureReading> externalTemperature = builder
                .stream(EXTERNAL_TEMP_TOPIC, with(Serdes.String(), readingsJsonSerde));

        Predicate<String, TimestampedTemperatureReading> validTemperatureFilter =
                (sensorId, reading) -> reading.getValue() > -15 && reading.getValue() < 60;

        internalTemperature
                .filter(validTemperatureFilter)
                .groupByKey(Grouped.valueSerde(readingsJsonSerde))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMillis(WINDOW_SIZE_MS)))
                .aggregate(() -> new DoubleStatistics(),
                        (sensorId, reading, aggStats) -> {
                            aggStats.setCount(aggStats.getCount() + 1);
                            aggStats.setSum(aggStats.getSum() + reading.getValue());
                            aggStats.setAverage(aggStats.getSum() / aggStats.getCount());
                            aggStats.setMin(min(aggStats.getMin(), reading.getValue()));
                            aggStats.setMax(max(aggStats.getMin(), reading.getValue()));
                            aggStats.setTimestamp(reading.getTimestamp());
                            return aggStats;
                        }, Materialized.with(Serdes.String(), doubleStatisticsSerde))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .mapValues(t -> String.format("Count:%3d, Sum:%10.5f, Avg:%9.5f,  Min:%9.5f,  Max:%9.5f, Time: %d",
                        t.getCount(), t.getSum(), t.getAverage(), t.getMin(), t.getMax(), t.getTimestamp()))
                .to(OUTPUT_TOPIC);

//        externalTemperature
//                .filter(validTemperatureFilter)
//                .to(OUTPUT_TOPIC);

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
