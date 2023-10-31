package org.iproduct.ksdemo.service;

import org.springframework.stereotype.Service;
import reactor.core.publisher.Sinks;

@Service
public class ReactiveRobotService {
    private Sinks.Many<String> sensorReadings = Sinks.many().multicast().directBestEffort(); //.replay().all();
    private Sinks.Many<String> commands = Sinks.many().multicast().directBestEffort(); //.replay().all();
    public Sinks.Many<String> getSensorReadings() {
        return sensorReadings;
    }
    public Sinks.Many<String> getCommands() {
        return commands;
    }


}
