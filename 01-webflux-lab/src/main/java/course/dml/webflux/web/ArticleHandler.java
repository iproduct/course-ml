package course.dml.webflux.web;

import course.dml.webflux.dao.ArticleRepository;
import course.dml.webflux.model.Article;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;

@Component
public class ArticleHandler {
    @Autowired
    private ArticleRepository articleRepo;

    public Mono<ServerResponse> getAll(ServerRequest req) {
        return ServerResponse.ok().body(articleRepo.findAll(), Article.class);
    }
}
