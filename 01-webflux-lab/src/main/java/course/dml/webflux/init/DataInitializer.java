package course.dml.webflux.init;

import course.dml.webflux.dao.ArticleRepository;
import course.dml.webflux.model.Article;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.util.List;

@Component
public class DataInitializer implements CommandLineRunner {
    @Autowired
    private ArticleRepository articleRepo;
    public static final List<Article> SAMPLE_ARTICLES = List.of(
            new Article("New Spring", "Web Flux is here ..."),
            new Article("DI in Action", "To @Autowire or not ..."),
            new Article("Reactive Programming with Spring", "Project Reactor provides reactive implementation ...")
    );

    @Override
    public void run(String... args) throws Exception {
        articleRepo
//                .deleteAll()
//                .thenMany( ...)
                .count()
                .log()
                .filter(count -> count == 0)
                .flatMapMany(count ->
                        Flux.fromIterable(SAMPLE_ARTICLES)
                                .flatMap(article -> articleRepo.insert(article))
                )
                .log()
                .subscribe(System.out::println, // next
                        System.err::println, // error
                        () -> System.out.println("Initialization done.")); //completion

    }
}
