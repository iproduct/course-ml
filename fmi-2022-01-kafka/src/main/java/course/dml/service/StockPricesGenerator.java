package course.dml.service;

import course.dml.model.StockPrice;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class StockPricesGenerator {
    public static final List<StockPrice> STOCKS = List.of(
        new StockPrice("VMW", "VMWare", 215.35),
        new StockPrice("GOOG", "Google", 309.17),
        new StockPrice("CTXS", "Citrix Systems Inc.", 112.11),
        new StockPrice("DELL", "Dell Inc.", 92.93),
        new StockPrice("MSFT", "Microsoft", 255.19),
        new StockPrice("ORCL", "Oracle", 115.72),
        new StockPrice("RHT", "Red Hat", 111.27)
    );

    public static Flux<StockPrice> getQuotesStream(long number, Duration period) {
        return Flux.interval(period)
                .take(number)
                .map(index -> {
                    var quote = STOCKS.get(index.intValue() % STOCKS.size());
                    quote.setPrice(quote.getPrice() * (0.9 + 0.2 * Math.random()));
                    return new StockPrice(index, quote.getSymbol(), quote.getName(), quote.getPrice(), new Date());
                })
                .log()
                .share();
    }

    public static void main(String[] args) throws InterruptedException {
        var latch = new CountDownLatch(2);
        var quotesStream = getQuotesStream(20, Duration.ofMillis(500));
        var s1 = quotesStream.subscribe(
                stockPrice -> System.out.printf("Subscriber 1: %s%n", stockPrice), // next
                System.err::println, // error
                () -> latch.countDown() // completion
        );
        TimeUnit.SECONDS.sleep(2);
        var s2 = quotesStream.subscribe(
                stockPrice -> System.out.printf("Subscriber 2: %s%n", stockPrice), // next
                System.err::println, // error
                () -> latch.countDown() // completion
        );
        latch.await();
        s1.dispose();
        s2.dispose();
        System.out.println("Stream demo complete.");
    }
}
