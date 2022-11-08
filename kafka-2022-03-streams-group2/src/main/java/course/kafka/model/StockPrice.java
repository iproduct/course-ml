package course.kafka.model;

import lombok.*;

import java.time.LocalDateTime;
import java.util.Date;

@Data
public class StockPrice implements Timestamped{
    private Long id;
    private String symbol;
    private String name;
    private Double price;
    private long timestamp = System.currentTimeMillis();

    public StockPrice() {
    }

    public StockPrice(String symbol, String name, Double price) {
        this.symbol = symbol;
        this.name = name;
        this.price = price;
    }

    public StockPrice(Long id, String symbol, String name, Double price, long timestamp) {
        this.id = id;
        this.symbol = symbol;
        this.name = name;
        this.price = price;
        this.timestamp = timestamp;
    }
}
