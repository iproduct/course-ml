package course.dml.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class TemperatureReadingsPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        final List<PartitionInfo> partitionInfos = cluster.availablePartitionsForTopic(topic);
        final int partitionCount = partitionInfos.size();
        final var keyStr = key.toString();
        Pattern pattern = Pattern.compile("[a-zA-Z]*-(\\d+)");
        var matcher = pattern.matcher(keyStr);
        if(matcher.matches()) {
            var sensorNumStr = matcher.group(1);
            var sensorNum = Integer.parseInt(sensorNumStr);
            return sensorNum % partitionCount;
        }
        return 0;
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
