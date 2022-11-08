package course.kafka.consumer;

import course.kafka.model.TemperatureReading;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class TemperatureIndividualConsumerTest {
    public static final String TOPIC = "temperature";
    public static final int PARTITION = 0;
    public static final long START_OFFSET = 0;
    public static final long POLLING_DURATION_MS = 100;
    public static final String TEMP_READING_ID = Uuid.randomUuid().toString();
    public static final LocalDateTime TEMP_READING_TIMESTAMP = LocalDateTime.now();
    public static final TemperatureReading SAMPLE_READING =
            new TemperatureReading(TEMP_READING_ID, "TempSensor01", 25.78, TEMP_READING_TIMESTAMP);

    private MockConsumer<String, TemperatureReading> consumer;
    private List<TemperatureReading> updates = new ArrayList<>();
    private TemperatureIndividualConsumer temperatureConsumer;
    private Throwable pollException;

    @BeforeEach
    void setUp() {
        consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        updates = new ArrayList<>();
        temperatureConsumer = new TemperatureIndividualConsumer(consumer,
                ex -> this.pollException = ex, updates::add, Duration.ofMillis(POLLING_DURATION_MS));
    }

    @Test
    @DisplayName("When startingByAssigning(TopicPartition),then updates are consumed correctly")
    void startByAssigning() {
        // GIVEN
        consumer.schedulePollTask(() -> consumer.addRecord(
                new ConsumerRecord<String, TemperatureReading>(TOPIC, PARTITION, START_OFFSET, TEMP_READING_ID, SAMPLE_READING)));
        consumer.schedulePollTask(() -> temperatureConsumer.stop());

        HashMap<TopicPartition, Long> startOffsets = new HashMap<>();
        TopicPartition tp = new TopicPartition(TOPIC, PARTITION);
        startOffsets.put(tp, 0L);
        consumer.updateBeginningOffsets(startOffsets);

        // WHEN
        temperatureConsumer.startByAssigning(TOPIC, PARTITION);

        // THEN
        assertThat(updates).hasSize(1);
        assertThat(consumer.closed()).isTrue();
    }

    @Test
    @DisplayName("When startingBySubscribing(Topic), then updates are consumed correctly")
    void startBySubscribing() {
        // GIVEN
        consumer.schedulePollTask(() -> {
            consumer.rebalance(Collections.singletonList(new TopicPartition(TOPIC, 0)));
            consumer.addRecord(
                    new ConsumerRecord<String, TemperatureReading>(TOPIC, PARTITION, START_OFFSET, TEMP_READING_ID, SAMPLE_READING));
        });
        consumer.schedulePollTask(() -> temperatureConsumer.stop());

        HashMap<TopicPartition, Long> startOffsets = new HashMap<>();
        TopicPartition tp = new TopicPartition(TOPIC, PARTITION);
        startOffsets.put(tp, 0L);
        consumer.updateBeginningOffsets(startOffsets);

        // WHEN
        temperatureConsumer.startBySubscribing(TOPIC);

        // THEN
        assertThat(updates).hasSize(1);
        assertThat(consumer.closed()).isTrue();
    }

    @Test
    @DisplayName("When startingBySubscribing(Topic) and Exception occurs, then expect that Exception is handled correctly")
    void whenStartingBySubscribingToTopicAndExceptionOccurs_thenExpectExceptionIsHandledCorrectly() {
        // GIVEN
        consumer.schedulePollTask(() -> consumer.setPollException(new KafkaException("polling exception")));
        consumer.schedulePollTask(() -> temperatureConsumer.stop());

        HashMap<TopicPartition, Long> startOffsets = new HashMap<>();
        TopicPartition tp = new TopicPartition(TOPIC, 0);
        startOffsets.put(tp, 0L);
        consumer.updateBeginningOffsets(startOffsets);

        // WHEN
        temperatureConsumer.startBySubscribing(TOPIC);

        // THEN
        assertThat(pollException).isInstanceOf(KafkaException.class).hasMessage("polling exception");
        assertThat(consumer.closed()).isTrue();
    }


    @Test
    @DisplayName("When startingBySubscribing(Topic) and WakeupException occurs, then expect consumer is canceled correctly")
    void stop() {
        // GIVEN
        consumer.schedulePollTask(() -> consumer.wakeup());

        HashMap<TopicPartition, Long> startOffsets = new HashMap<>();
        TopicPartition tp = new TopicPartition(TOPIC, 0);
        startOffsets.put(tp, 0L);
        consumer.updateBeginningOffsets(startOffsets);

        // WHEN
        temperatureConsumer.startBySubscribing(TOPIC);

        // THEN
        assertThat(pollException).isNull();
        assertThat(updates).hasSize(0);
        assertThat(consumer.closed()).isTrue();
    }
}
