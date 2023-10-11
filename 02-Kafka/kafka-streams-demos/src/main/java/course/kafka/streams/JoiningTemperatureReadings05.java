package course.kafka.streams;

import course.kafka.model.DoubleStatistics;
import course.kafka.model.TempDifference;
import course.kafka.model.TimestampedTemperatureReading;
import course.kafka.serialization.JsonDeserializer;
import course.kafka.serialization.JsonSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static java.lang.Double.max;
import static java.lang.Double.min;
import static org.apache.kafka.streams.kstream.Consumed.with;


public class JoiningTemperatureReadings05 {
    public static final String INTERNAL_TEMP_TOPIC = "temperature";
    public static final String EXTERNAL_TEMP_TOPIC = "external-temperature";
    public static final String OUTPUT_TOPIC = "events";
    public static final long WINDOW_SIZE_MS = 5000;

    // create custom JSON Serdes and filters
    private static Serde<TimestampedTemperatureReading> readingsJsonSerde = Serdes.serdeFrom(
            new JsonSerializer<>(), new JsonDeserializer<>(TimestampedTemperatureReading.class));
    private static Serde<DoubleStatistics> doubleStatisticsSerde = Serdes.serdeFrom(
            new JsonSerializer<>(), new JsonDeserializer<>(DoubleStatistics.class));
    private static Serde<TempDifference> tempDifferenceSerde = Serdes.serdeFrom(
            new JsonSerializer<>(), new JsonDeserializer<>(TempDifference.class));
    private static Predicate<String, TimestampedTemperatureReading> validTemperatureFilter =
            (sensorId, reading) -> reading.getValue() > -15 && reading.getValue() < 60;

    public static KStream<String, DoubleStatistics> createTemperatureStatisticsStream(StreamsBuilder builder, String inputTopic) {
        return builder.stream(inputTopic, with(Serdes.String(), readingsJsonSerde))
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
                .map((windowedSensorId, stats) -> new KeyValue<>(windowedSensorId.key(), stats));
    }

    public static void main(String[] args) {
        // 1) Configure stream
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "heating-bills");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once_v2");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // 2) Create stream builder
        final StreamsBuilder builder = new StreamsBuilder();
        var internalTemperature = createTemperatureStatisticsStream(builder, INTERNAL_TEMP_TOPIC);
        var externalTemperature = createTemperatureStatisticsStream(builder, EXTERNAL_TEMP_TOPIC);

        internalTemperature
                .join(externalTemperature, (s1, s2) ->
                        new TempDifference(max(s1.getAverage() - s2.getAverage(), 0), Long.max(s1.getTimestamp(), s2.getTimestamp())),
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMillis(1000)),
//                        StreamJoined.<String, DoubleStatistics, DoubleStatistics>as("join-internal-external-temperatures")
//                                .withValueSerde(doubleStatisticsSerde).withOtherValueSerde(doubleStatisticsSerde))
                        StreamJoined.with(Serdes.String(), doubleStatisticsSerde, doubleStatisticsSerde)
                                .withName("join-internal-external-temperatures"))
                .groupByKey(Grouped.valueSerde(tempDifferenceSerde))
                .aggregate(() -> new TempDifference(),
                        (sensorId, tempDiff, aggPower) -> {
                            if(aggPower.getTimestamp() == 0L) {
                                aggPower.setTimestamp(tempDiff.getTimestamp());
                            } else {
                                aggPower.setValue(aggPower.getValue() +
                                        tempDiff.getValue() * (tempDiff.getTimestamp() - aggPower.getTimestamp()));
                                aggPower.setTimestamp(tempDiff.getTimestamp());
                            }
                            return aggPower;
                        }, Materialized.with(Serdes.String(), tempDifferenceSerde))
                .toStream()
                .mapValues((k, t) -> String.format("SENSOR-ID: %12s -> CONSUMED HEATING POWER:%9.5f, Time: %d", k, t.getValue(), t.getTimestamp()))
                .to(OUTPUT_TOPIC);

        // 3) Build stream topology
        final Topology topology = builder.build(); // build DAG
        System.out.println(topology.describe());

        // 4) Create streams instance
        final KafkaStreams streams = new KafkaStreams(topology, props);
//        streams.cleanUp();
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
