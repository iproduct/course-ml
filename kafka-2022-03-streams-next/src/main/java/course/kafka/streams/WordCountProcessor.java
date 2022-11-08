package course.kafka.streams;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;

public class WordCountProcessor implements Processor<String, String, String, String> {
    private KeyValueStore<String, Long> kvStore;

    @Override
    public void init(ProcessorContext<String, String> context) {
        context.schedule(Duration.ofSeconds(1), PunctuationType.STREAM_TIME, timestamp -> {
            try (final KeyValueIterator<String, Long> iter = kvStore.all()) {
                while (iter.hasNext()) {
                    final KeyValue<String, Long> entry = iter.next();
                    context.forward(new Record<>(entry.key, entry.value.toString(), timestamp));
                }
            }
        });
        kvStore = context.getStateStore("Counts");
    }

    @Override
    public void process(Record<String, String> record) {
        final String[] words = record.value().toLowerCase().split("\\W+");

        for(final String word : words) {
            final Long oldVal = kvStore.get(word);
            if(oldVal == null){
                kvStore.put(word, 1L);
            } else {
                kvStore.put(word, oldVal + 1);
            }
        }

    }

    @Override
    public void close() {
    }
}
