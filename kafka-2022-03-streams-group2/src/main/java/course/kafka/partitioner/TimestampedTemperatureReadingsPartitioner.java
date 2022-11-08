package course.kafka.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import reactor.core.publisher.Flux;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static course.kafka.model.TimestampedTemperatureReading.SENSOR_IDS;
import static course.kafka.service.StockPricesGenerator.STOCKS;

public class TimestampedTemperatureReadingsPartitioner implements Partitioner {
    private static final Map<String, Integer> symbolToOrdinalMap = new HashMap<>();

    public TimestampedTemperatureReadingsPartitioner() {
        Flux.range(0, SENSOR_IDS.size()).zipWith(Flux.fromIterable(SENSOR_IDS))
                .subscribe((t) -> symbolToOrdinalMap.put(t.getT2(), t.getT1()));

    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        final List<PartitionInfo> partitionInfos = cluster.availablePartitionsForTopic(topic);
        final int partitionCount = partitionInfos.size();
        final var keyStr = key.toString();
        var sensorId = symbolToOrdinalMap.get(keyStr);
        if(sensorId == null) return 0;
        return sensorId % partitionCount;
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
