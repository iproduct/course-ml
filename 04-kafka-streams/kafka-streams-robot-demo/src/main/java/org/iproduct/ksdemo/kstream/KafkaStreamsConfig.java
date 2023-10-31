package org.iproduct.ksdemo.kstream;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.UsePartitionTimeOnInvalidTimestamp;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.iproduct.ksdemo.kstream.serialization.JsonDeserializer;
import org.iproduct.ksdemo.kstream.serialization.JsonSerializer;
import org.iproduct.ksdemo.model.DistanceReading;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofMinutes;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.maxBytes;
import static org.apache.kafka.streams.kstream.Suppressed.untilTimeLimit;
import static org.apache.kafka.streams.kstream.Suppressed.untilWindowCloses;

@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaStreamsConfig {
    // create custom JSON Serdes
    private static Serde<DistanceReading> readingsJsonSerde = Serdes.serdeFrom(
            new JsonSerializer<>(), new JsonDeserializer<>(DistanceReading.class));

    @Value(value = "${spring.kafka.bootstrap-servers}")
    private String bootstrapAddress;

    //    public static final String TOPIC = "prices";
//    public static final String CLIENT_ID = "TestProducer";
//    public static final String BOOTSTRAP_SERVERS = "localhost:9093";
//    public static final long MAX_DEMO_TIME_MS = 120_000;
//    public static final long NUM_PRODUCERS = 1;
    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "testStreams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0);

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, UsePartitionTimeOnInvalidTimestamp.class.getName());
        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    public StreamsBuilderFactoryBeanConfigurer configurer() {
        return fb -> fb.setStateListener((newState, oldState) -> {
            System.out.println("State transition from " + oldState + " to " + newState);
        });
    }

    @Bean
    public KStream<Integer, DistanceReading> kStream(StreamsBuilder kStreamBuilder) {
        KStream<Integer, DistanceReading> stream = kStreamBuilder.stream("sweepDistances", Consumed.with(Serdes.Integer(), readingsJsonSerde));
        stream
//                .mapValues((ValueMapper<String, String>) String::toUpperCase)
                .filter((i, r) -> r != null && r.getType().equals("distance"))
                .groupByKey()
                .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofMillis(1000)))
                //  .reduce((String value1, String value2) -> value1 + value2) //,  Named.as("windowStore"))
                .reduce((DistanceReading value1, DistanceReading value2) ->
                        value1.getDistance() < value2.getDistance() ? value1 : value2)
//                .suppress(untilTimeLimit(ofMillis(200), maxBytes(1_000L).emitEarlyWhenFull()))
//                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .filter((key, value) -> value != null)
                .map((windowedId, value) -> new KeyValue<>(windowedId.key(), String.format("Type:%s, Angle:%3.0f, Distance:%9.5f, Time: %d",
                        value.getType(), value.getAngle(), value.getDistance(), value.getTime())))
//                .filter((i, s) -> s != null && s.endsWith("{\"type\":\"sweep_end\"}"))
                .to("minSweepDistance");

        stream.print(Printed.toSysOut());

        return stream;
    }

}


