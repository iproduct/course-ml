package course.kafka.metrics;

import lombok.Value;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

@Value
public class MetricPair {
    private MetricName metricName;
    private Metric metric;

    @Override
    public String toString() {
        return metricName.group() + "." + metricName.name();
    }
}
