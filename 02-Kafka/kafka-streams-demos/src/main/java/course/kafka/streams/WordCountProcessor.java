package course.kafka.streams;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

public class WordCountProcessor implements Processor<String, String, String, String> {
    private KeyValueStore<String, Long> kvStore;
    private ProcessorContext<String, String> context;

    @Override
    public void init(ProcessorContext<String, String> context) {
        this.context = context;
//        context.schedule(Duration.ofSeconds(1), PunctuationType.STREAM_TIME, timestamp -> {
//            try (final KeyValueIterator<String, Long> iter = kvStore.all()) {
//                while (iter.hasNext()) {
//                    final KeyValue<String, Long> entry = iter.next();
//                    context.forward(new Record<>(
//                            entry.key,
//                            String.format("%-15s -> %4d", entry.key, entry.value),
//                            timestamp
//                    ));
//                }
//            }
//        });
        kvStore = context.getStateStore("inmemory-word-counts");
    }

    @Override
    public void process(Record<String, String> record) {
        final String[] words = record.value().toLowerCase().split("\\W+");

        for (final String word : words) {
            Long oldVal = kvStore.get(word);
            if (oldVal == null) {
                oldVal = 0L;
            }
            kvStore.put(word, oldVal + 1);
            context.forward(new Record<>(
                    word,
                    String.format("%-15s -> %4d", word, oldVal + 1),
                    record.timestamp()
            ));
        }
    }

    @Override
    public void close() {
    }
}
