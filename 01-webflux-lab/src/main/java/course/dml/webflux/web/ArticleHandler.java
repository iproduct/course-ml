package course.dml.webflux.web;

import course.dml.webflux.dao.ArticleRepository;
import course.dml.webflux.model.Article;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;

import java.net.URI;

@Component
public class ArticleHandler {
    @Autowired
    private ArticleRepository articleRepo;

    public Mono<ServerResponse> getAll(ServerRequest req) {
        return ServerResponse.ok().body(articleRepo.findAll(), Article.class);
    }

    public Mono<ServerResponse> create(ServerRequest req) {
        return req.bodyToMono(Article.class)
                .flatMap(article -> articleRepo.insert(article))
                .flatMap(newArticle -> ServerResponse.created(
                        URI.create("/articles/" + newArticle.getId()))
                        .bodyValue(newArticle)
                );
    }
}
