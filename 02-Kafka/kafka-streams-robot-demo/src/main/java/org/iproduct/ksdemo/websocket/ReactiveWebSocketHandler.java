package org.iproduct.ksdemo.websocket;

import org.iproduct.ksdemo.service.ReactiveRobotService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Duration;

import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

@Component
public class ReactiveWebSocketHandler implements WebSocketHandler {

    private Flux<String> intervalFlux = Flux.interval(Duration.ofMillis(1000)).map(n -> "" + n);

    @Autowired
    private ReactiveRobotService robotService;

    @Override
    public Mono<Void> handle(WebSocketSession webSocketSession) {
        return webSocketSession.send(robotService.getSensorReadings().asFlux().map(webSocketSession::textMessage))
                .and(webSocketSession.receive()
                        .map(message -> message.getPayloadAsText())
                        .log()
                        .doOnNext(command -> robotService.getCommands().emitNext(command, FAIL_FAST))
                );
    }
}
