package course.dml.webflux.reactor;

import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.stream.Stream;

public class ReactorDemo {
    public static void main(String[] args) throws InterruptedException, IOException {
        Flux<Long> moments = Flux.interval(Duration.ofMillis(100));
//        moments.subscribe(event -> {
//            System.out.println(event);
//        });
//        moments.map(i -> i*i).subscribe(System.out::println);

        // read words from file
        Stream<String> words = Files.lines(Paths.get("src/main/resources/words.txt"))
                .flatMap(line -> Arrays.asList(line.split(" ")).stream());
        Flux<Tuple2<String, Long>> tuples = Flux.fromStream(words)
                .zipWith(moments);
        tuples.filter(s -> Character.isUpperCase(s.getT1().charAt(0)))
                .map(t2 -> t2.getT2() + ": " + t2.getT1() + ", ")
                .subscribe(System.out::print, err -> System.out.printf("!!! Error: %s%n", err));


        Thread.sleep(60000);
    }
}
