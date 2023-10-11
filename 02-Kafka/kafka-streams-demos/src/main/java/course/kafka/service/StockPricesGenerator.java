package course.kafka.service;

import course.kafka.model.StockPrice;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


public class StockPricesGenerator {
    private static Random rand = new Random();

    public static final List<StockPrice> STOCKS = Arrays.asList(
            new StockPrice("VMW", "VMWare", 215.35),
            new StockPrice("GOOG", "Google", 309.17),
            new StockPrice("CTXS", "Citrix Systems, Inc.", 112.11),
            new StockPrice("DELL", "Dell Inc.", 92.93),
            new StockPrice("MSFT", "Microsoft", 255.19),
            new StockPrice("ORCL", "Oracle", 115.72),
            new StockPrice("RHT", "Red Hat", 111.27)
    );

    public static Flux<StockPrice> getQuotesStream(long number, Duration period) {
        return Flux.interval(period)
                .take(number)
                .map(index -> {
                    StockPrice quote = STOCKS.get(index.intValue() % STOCKS.size());
                    quote.setPrice(quote.getPrice() * (0.9 + 0.2 * rand.nextDouble()));
                    return new StockPrice(index, quote.getSymbol(), quote.getName(), quote.getPrice(), new Date());
                })
//                .log()
                .share();
    }

    public static void main(String[] args) throws InterruptedException {
        var latch = new CountDownLatch(2);
        var quotesStream = getQuotesStream(20, Duration.ofMillis(500));
        var disp1 = quotesStream.subscribe(
                sp -> System.out.printf("Subscriber 1: %s%n", sp),  // next
                System.err::println, //error
                () -> latch.countDown() //completion
        );
        TimeUnit.SECONDS.sleep(2);
        var disp2 = quotesStream.subscribe(
                sp -> System.out.printf("Subscriber 2: %s%n", sp),  // next
                System.err::println, //error
                () -> latch.countDown() //completion
        );
        latch.await();
        disp1.dispose();
        disp2.dispose();
        System.out.println("Stream completed");
    }
}
