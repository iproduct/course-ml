package course.dml.webflux.web;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/hello")
public class HelloWebFluxController {
    @GetMapping
    private String sayHello(){
        return "Hello from Web Flux!";
    }
}
