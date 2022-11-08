package course.kafka.consumer;

import course.kafka.model.TemperatureReading;
import course.kafka.serialization.JsonDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.stream.StreamSupport;

@Slf4j
public class TemperatureIndividualConsumer {
    private Consumer<String, TemperatureReading> consumer;
    private java.util.function.Consumer<Throwable> exceptionConsumer;
    private java.util.function.Consumer<TemperatureReading> temperatureConsumer;
    private volatile boolean canceled;

    private Duration pollingInterval;

    public TemperatureIndividualConsumer(Consumer<String, TemperatureReading> consumer,
                                         java.util.function.Consumer<Throwable> exceptionConsumer,
                                         java.util.function.Consumer<TemperatureReading> temperatureConsumer,
                                         Duration pollingInterval) {
        this.consumer = consumer;
        this.exceptionConsumer = exceptionConsumer;
        this.temperatureConsumer = temperatureConsumer;
        this.pollingInterval = pollingInterval;
    }

    void startBySubscribing(String topic) {
        consume(() -> consumer.subscribe(Collections.singleton(topic)));
    }

    void startByAssigning(String topic, int partition) {
        consume(() -> consumer.assign(Collections.singleton(new TopicPartition(topic, partition))));
    }

    private void consume(Runnable beforePollingTask) {
        try {
            beforePollingTask.run();
            while (!canceled) {
                ConsumerRecords<String, TemperatureReading> records = consumer.poll(pollingInterval);
                StreamSupport.stream(records.spliterator(), false)
                        .map(record -> record.value())
                        .forEach(temperatureConsumer);
                consumer.commitSync();
            }
        } catch (WakeupException e) {
            System.out.println("Shutting down...");
        } catch (RuntimeException ex) {
            exceptionConsumer.accept(ex);
        } finally {
            consumer.close();
        }
    }

    public void stop() {
        canceled = true;
        consumer.wakeup();
    }
}

