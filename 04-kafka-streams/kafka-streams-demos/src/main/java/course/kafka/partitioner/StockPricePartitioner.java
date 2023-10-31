package course.kafka.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import reactor.core.publisher.Flux;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static course.kafka.service.StockPricesGenerator.STOCKS;

public class StockPricePartitioner implements Partitioner {
    public static final int NUMBER_OF_PARTITIONS_PER_HF_SENSOR = 3;
    private List<String> highFrequencySensorsIds;

    private static final Map<String, Integer> symbolToOrdinalMap = new HashMap<>();

    public StockPricePartitioner() {
        Flux.range(0, STOCKS.size()).zipWith(Flux.fromIterable(STOCKS))
                .subscribe((t) -> symbolToOrdinalMap.put(t.getT2().getSymbol(), t.getT1()));

    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        final List<PartitionInfo> partitionInfos = cluster.availablePartitionsForTopic(topic);
        final int partitionCount = partitionInfos.size();
        final var keyStr = key.toString();
        return symbolToOrdinalMap.get(keyStr) % partitionCount;
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
