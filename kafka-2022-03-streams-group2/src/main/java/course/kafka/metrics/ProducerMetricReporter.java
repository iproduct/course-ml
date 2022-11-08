package course.kafka.metrics;

import course.kafka.model.TemperatureReading;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Sensor;

import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static course.kafka.producer.SimpleTemperatureReadingsProducer.MY_MESSAGE_SIZE_SENSOR;

@Slf4j
public class ProducerMetricReporter implements Callable<String> {
    private final Producer<String, TemperatureReading> producer;
    private long metricsSnapshotRateMs = 3000;

    public ProducerMetricReporter(Producer<String, TemperatureReading> producer) {
        this.producer = producer;
    }

    public ProducerMetricReporter(Producer<String, TemperatureReading> producer, long metricsSnapshotRateMs) {
        this.producer = producer;
        this.metricsSnapshotRateMs = metricsSnapshotRateMs;
    }

    // filter metrics by name
    private final Set<String> metricNames = Set.of(
            "record-queue-time-avg", "record-send-rate", "records-per-request-avg",
            "request-size-max", "network-io-rate", "network-io-total",
            "incoming-byte-rate", "batch-size-avg", "requests-in-flight",
            "request-rate", "response-rate", "request-latency-avg", "request-latency-max",
            MY_MESSAGE_SIZE_SENSOR+"-avg", MY_MESSAGE_SIZE_SENSOR+"-min", MY_MESSAGE_SIZE_SENSOR+"-max");


    @Override
    public String call() throws Exception {
        while (true) {
            final var metrics = producer.metrics();
            displayMetrics(metrics);
            try {
                TimeUnit.MILLISECONDS.sleep(metricsSnapshotRateMs);
            } catch (InterruptedException ex) {
                log.warn("Metrics interupted for producer: " + producer);
                Thread.interrupted();
                break;
            }
        }

        return producer.toString();
    }

    private void displayMetrics(Map<MetricName, ? extends Metric> metrics) {
        final Map<String, MetricPair> metricsDisplay =
                metrics.entrySet().stream()
                        .filter(entry -> metricNames.contains(entry.getKey().name()))
//                        .filter(entry -> {
//                            var val = entry.getValue().metricValue();
//                            if (!(val instanceof Double)) return false;
//                            var doubleVal = (double) val;
//                            return Double.isFinite(doubleVal) && !Double.isNaN(doubleVal) && doubleVal > 0;
//                        })
                        .map(entry -> new MetricPair(entry.getKey(), entry.getValue()))
                        .collect(Collectors.toMap(MetricPair::toString, Function.identity(), (a, b) -> b, TreeMap::new));
        var sj = new StringJoiner("\n",
                "\n-------------------------------------------------------------------------------------------------------\n",
                "\n-------------------------------------------------------------------------------------------------------\n");
        metricsDisplay.forEach((name, pair) -> {
            sj.add(String.format("| %-40.40s | %-20.20s | %-10.2f | %-60.60s |",
                    name,
                    pair.getMetricName().name(),
                    (double) pair.getMetric().metricValue(),
                    pair.getMetricName().description()
            ));
        });
        log.info(sj.toString());
    }
}
