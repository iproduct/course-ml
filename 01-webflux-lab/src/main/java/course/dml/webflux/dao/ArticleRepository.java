package course.dml.webflux.dao;

import course.dml.webflux.model.Article;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;

@Repository
public interface ArticleRepository extends ReactiveMongoRepository<Article, String> {
    Flux<Article> findByTitle(String title);
}
