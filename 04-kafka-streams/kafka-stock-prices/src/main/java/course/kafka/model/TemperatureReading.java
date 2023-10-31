package course.kafka.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TemperatureReading {
    public static final List<String> HF_SENSOR_IDS = List.of(
            "tHighFrequency-01", "tHighFrequency-02"
    );
    public static final List<String> NORMAL_SENSOR_IDS = List.of(
            "tSensor-01", "tSensor-02", "tSensor-03", "tSensor-04", "tSensor-04", "tSensor-05",
            "tSensor-06", "tSensor-07", "tSensor-08", "tSensor-09", "tSensor-10", "tSensor-11",
            "tSensor-12", "tSensor-13", "tSensor-14", "tSensor-15", "tSensor-16", "tSensor-17"
    );
    private String id;
    private String sensorId;
    private double value;
    private LocalDateTime timestamp = LocalDateTime.now();

    public TemperatureReading(String id, String sensorId, double value) {
        this.id = id;
        this.sensorId = sensorId;
        this.value = value;
    }
}


