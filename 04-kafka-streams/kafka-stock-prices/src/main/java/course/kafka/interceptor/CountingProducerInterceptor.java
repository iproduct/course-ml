package course.kafka.interceptor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

@Slf4j
public class CountingProducerInterceptor<K,V> implements ProducerInterceptor<K,V>, Runnable {
    public static final String REPORTING_WINDOW_SIZE_MS = "interceptor.reporting.window.size.ms";
    public static final long DEFAULT_REPORTING_WINDOW_SIZE_MS = 5000;
    private static ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private static Map<Set<Integer>, MetricsTuple> metricsMap = new ConcurrentHashMap<>();
    private Set<Integer> partitions = new ConcurrentSkipListSet<>();

    @Override
    public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {
        var tuple = metricsMap.getOrDefault(partitions, new MetricsTuple());
        tuple.getSent().incrementAndGet();
        metricsMap.putIfAbsent(partitions, tuple);
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        partitions.add(metadata.partition());
        var tuple = metricsMap.getOrDefault(partitions, new MetricsTuple());
        if(exception == null){
            tuple.getAcknowledged().incrementAndGet();
        } else {
            tuple.getErrors().incrementAndGet();
        }
        metricsMap.putIfAbsent(partitions, tuple);
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        long windowSize = DEFAULT_REPORTING_WINDOW_SIZE_MS;
        try {
            windowSize = Long.parseLong(String.valueOf(configs.get(REPORTING_WINDOW_SIZE_MS)));
        } catch (NumberFormatException ex){
            log.error("Error: Invalid interceptor config property: " + REPORTING_WINDOW_SIZE_MS, ex);
        }
        executor.scheduleAtFixedRate(this, windowSize, windowSize, TimeUnit.MILLISECONDS);
    }

    @Override
    public void run() {
        if(metricsMap.get(partitions) != null && metricsMap.get(partitions).isNotZero()) {
            var tuple = metricsMap.get(partitions);
            var message = String.format("\t| Number Records/Acks/Errors | %15.15s | %6d | %6d | %6d |",
                    partitions, tuple.getSentValue(), tuple.getAcknowledgedValue(), tuple.getErrorsValue() );
            log.info(message);
            tuple.setZero();
        }
    }
}
