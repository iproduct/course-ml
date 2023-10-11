package course.kafka.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import reactor.core.publisher.Flux;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static course.kafka.model.TemperatureReading.NORMAL_SENSOR_IDS;

public class TemperatureReadingsPartitionerBySensorId implements Partitioner {

    private static final Map<String, Integer> sensorIdToOrdinalMap = new HashMap<>();

    public TemperatureReadingsPartitionerBySensorId() {
        Flux.range(0, NORMAL_SENSOR_IDS.size()).zipWith(Flux.fromIterable(NORMAL_SENSOR_IDS))
                .subscribe((t) -> sensorIdToOrdinalMap.put(t.getT2(), t.getT1()));
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        final List<PartitionInfo> partitionInfos = cluster.availablePartitionsForTopic(topic);
        final int partitionCount = partitionInfos.size();
        final var keyStr = key.toString();
        if(sensorIdToOrdinalMap.get(keyStr) == null) return 0;
        return sensorIdToOrdinalMap.get(keyStr) % partitionCount;
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
