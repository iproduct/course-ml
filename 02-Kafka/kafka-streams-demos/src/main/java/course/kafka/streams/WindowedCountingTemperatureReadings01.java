package course.kafka.streams;

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

import static org.apache.kafka.streams.kstream.Consumed.with;


public class WindowedCountingTemperatureReadings01 {
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

        // create custom JSON Serde
        Serde<TimestampedTemperatureReading> jsonSerde = Serdes.serdeFrom(
                new JsonSerializer<>(), new JsonDeserializer<>(TimestampedTemperatureReading.class));

        // 2) Create stream builder
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, TimestampedTemperatureReading> internalTemperature = builder
                .stream(INTERNAL_TEMP_TOPIC, with(Serdes.String(), jsonSerde));
        KStream<String, TimestampedTemperatureReading> externalTemperature = builder
                .stream(EXTERNAL_TEMP_TOPIC, with(Serdes.String(), jsonSerde));

        Predicate<String, TimestampedTemperatureReading> validTemperatureFilter =
                (sensorId, reading) -> reading.getValue() > -15 && reading.getValue() < 60;

        internalTemperature
                .filter(validTemperatureFilter)
                .mapValues(reading -> reading.getValue())
                .groupByKey(Grouped.valueSerde(Serdes.Double()))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMillis(WINDOW_SIZE_MS)))
                .count(Materialized.with(Serdes.String(), Serdes.Long()))
                .toStream()
                .mapValues(t -> String.format("Temp: %5d", t))
//                .mapValues(t -> String.format("Temp: %9.5f", t))
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
