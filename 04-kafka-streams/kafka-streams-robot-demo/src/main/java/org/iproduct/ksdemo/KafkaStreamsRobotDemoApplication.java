package org.iproduct.ksdemo;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.eclipse.californium.core.coap.Request;
import org.eclipse.californium.core.coap.Response;
import org.eclipse.californium.core.network.CoapEndpoint;
import org.iproduct.ksdemo.service.ReactiveRobotService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.eclipse.californium.core.CoapResource;
import org.eclipse.californium.core.CoapServer;
import org.eclipse.californium.core.server.resources.CoapExchange;

import static org.eclipse.californium.core.coap.CoAP.ResponseCode.*;
import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

import java.net.InetSocketAddress;

@SpringBootApplication
@Slf4j
public class KafkaStreamsRobotDemoApplication {
    public static final String SERVER_IP = "192.168.1.102";
    public static final int COAP_PORT = 5683;
    public static final String ROBOT_IP = "192.168.1.103";

    @Autowired
    private KafkaTemplate<Integer, String> template;

    @Autowired
    private ReactiveRobotService robotService;


    public static void main(String[] args) {
        SpringApplication.run(KafkaStreamsRobotDemoApplication.class, args);
    }

    @Bean
    public ApplicationRunner runner() {
        return args -> {
            // binds on UDP port 5683
            CoapServer server = new CoapServer();
            server.addEndpoint(CoapEndpoint.builder().setInetSocketAddress(new InetSocketAddress(SERVER_IP, COAP_PORT)).build());

            server.add(new HelloResource());
            server.add(new TimeResource());
            server.add(new SensorsResource());

            server.start();
            robotService.getCommands().asFlux().subscribe(command -> {
                try {
                    Request request = Request.newPut();
                    request.setURI("coap://" + ROBOT_IP + ":5683/commands");
                    request.setPayload(command);
                    request.send();
                    Response response = request.waitForResponse(1000);
                    log.info("!!! Received: " + response);
                    if(response != null) {
                        robotService.getSensorReadings().emitNext(String.format("{\"type\":\"command_ack\", \"command\":\"%s\"}", response.getPayloadString()), FAIL_FAST);
                    }
                } catch (Exception e) {
                    log.error("Error sending command to robot: {}", e);
                    robotService.getSensorReadings().emitNext(String.format("{\"error\":\"%s\"}", e.getMessage()), FAIL_FAST);
                }
            });
        };
    }

    public static class HelloResource extends CoapResource {
        public HelloResource() {
            // resource identifier
            super("hello");
            // set display name
            getAttributes().setTitle("Hello-World Resource");
        }

        @Override
        public void handleGET(CoapExchange exchange) {
            exchange.respond("Hello world!");
        }
    }

    public static class TimeResource extends CoapResource {
        public TimeResource() {
            super("time");
        }

        @Override
        public void handleGET(CoapExchange exchange) {
            log.info("Received request from {}:{}", exchange.getSourceAddress(), exchange.getSourcePort());
            exchange.respond(String.valueOf(System.currentTimeMillis()));
        }
    }

    public class SensorsResource extends CoapResource {
        private String value = "";

        public SensorsResource() {
            // resource identifier
            super("sensors");
            // set display name
            getAttributes().setTitle("Sensors Resource");
        }

        @Override
        public void handleGET(CoapExchange exchange) {
            exchange.respond(value);
        }

        @Override
        public void handlePUT(CoapExchange exchange) {
            byte[] payload = exchange.getRequestPayload();

            try {
                value = new String(payload, "UTF-8");
                log.info("Received request: {} - {}:{}", value, exchange.getSourceAddress(), exchange.getSourcePort());
                robotService.getSensorReadings().emitNext(value, FAIL_FAST);
                template.send(new ProducerRecord<>("sweepDistances", 1, value));
                exchange.respond(CHANGED, value);
            } catch (Exception e) {
                e.printStackTrace();
                exchange.respond(BAD_REQUEST, "Invalid String");
            }
        }
    }
}
