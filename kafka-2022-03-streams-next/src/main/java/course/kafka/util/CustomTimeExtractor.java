package course.kafka.util;

import course.kafka.model.Timestamped;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

@Slf4j
public class CustomTimeExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
        final long timestamp = record.timestamp();

        // `TemperatureReading` is your own custom class, which we assume has a method that returns
        // the embedded timestamp (in milliseconds).
        var myReading = (Timestamped) record.value();
        if (myReading != null) {
            return myReading.getTimestamp();
        } else {
            // Kafka allows `null` as message value.  How to handle such message values
            // depends on your use case. Attempt to estimate a new timestamp,
            // otherwise fall back to wall-clock time (processing-time).
            if (previousTimestamp >= 0) {
                return previousTimestamp;
            } else {
                return System.currentTimeMillis();
            }
        }
    }

}
