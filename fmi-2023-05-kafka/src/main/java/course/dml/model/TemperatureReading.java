package course.dml.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TemperatureReading {
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


