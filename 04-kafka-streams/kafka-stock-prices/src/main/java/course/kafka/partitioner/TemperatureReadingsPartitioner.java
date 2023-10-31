package course.kafka.partitioner;

import course.kafka.model.TemperatureReading;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static course.kafka.model.TemperatureReading.NORMAL_SENSOR_IDS;
import static course.kafka.producer.SimpleTemperatureReadingsProducer.HIGH_FREQUENCY_SENSORS;

public class TemperatureReadingsPartitioner implements Partitioner {
    public static final int NUMBER_OF_PARTITIONS_PER_HF_SENSOR = 3;
    private List<String> highFrequencySensorsIds;

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        final List<PartitionInfo> partitionInfos = cluster.availablePartitionsForTopic(topic);
        final int partitionCount = partitionInfos.size();
        final int hfSensorsCount = highFrequencySensorsIds.size();
        final int normalSensorPartitions = partitionCount - hfSensorsCount * NUMBER_OF_PARTITIONS_PER_HF_SENSOR;
        final var keyStr = key.toString();
        final TemperatureReading valueReading = (TemperatureReading) value;
        final String sensorId = valueReading.getSensorId();
        final int hfReadingIndex = highFrequencySensorsIds.indexOf(sensorId);

        int partition = 0;
        if (hfReadingIndex >= 0) {
            partition = Integer.remainderUnsigned(Utils.murmur2(keyBytes), NUMBER_OF_PARTITIONS_PER_HF_SENSOR)
                    + hfReadingIndex * NUMBER_OF_PARTITIONS_PER_HF_SENSOR;
        } else {
            final int sensorIdIndex = NORMAL_SENSOR_IDS.indexOf(sensorId);
            if(sensorIdIndex >= 0) {
                partition = Integer.remainderUnsigned(sensorIdIndex, normalSensorPartitions)
                        + hfSensorsCount * NUMBER_OF_PARTITIONS_PER_HF_SENSOR;
            } else {
                partition = partitionCount - 1;
            }
        }
        return partition;
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
        var hfSensorIdsStr = (String) configs.get(HIGH_FREQUENCY_SENSORS);
        highFrequencySensorsIds =
                Arrays.stream(hfSensorIdsStr.split(",")).collect(Collectors.toList());
    }
}
