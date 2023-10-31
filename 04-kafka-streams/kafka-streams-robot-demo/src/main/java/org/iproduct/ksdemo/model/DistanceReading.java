package org.iproduct.ksdemo.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DistanceReading {
    private String type;
    private double angle;
    private double distance;
    private long time;
}
