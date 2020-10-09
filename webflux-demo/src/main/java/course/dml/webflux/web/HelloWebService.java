package course.dml.webflux.web;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping({"/hello/{name}", "/hello"})
public class HelloWebService {
    @GetMapping
    public String hello(@PathVariable(name = "name", required = false) String name) {
        return "Hello from Reactor, " + (name == null ? "Stranger": name) + "!";
    }
}
