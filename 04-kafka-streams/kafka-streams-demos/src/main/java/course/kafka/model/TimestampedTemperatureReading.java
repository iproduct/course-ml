package course.kafka.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TimestampedTemperatureReading {
    public static final List<String> HF_SENSOR_IDS = List.of(
            "tHighFrequency-01", "tHighFrequency-02"
    );
    public static final List<String> NORMAL_SENSOR_IDS = List.of(
            "tSensor-01", "tSensor-02", "tSensor-03", "tSensor-04", "tSensor-04", "tSensor-05",
            "tSensor-06", "tSensor-07", "tSensor-08", "tSensor-09", "tSensor-10", "tSensor-11",
            "tSensor-12", "tSensor-13", "tSensor-14", "tSensor-15", "tSensor-16", "tSensor-17"
    );
    private String sensorId;
    private double value;
    private long timestamp = System.currentTimeMillis();

    public TimestampedTemperatureReading(String sensorId, double value) {
        this.sensorId = sensorId;
        this.value = value;
    }
}


