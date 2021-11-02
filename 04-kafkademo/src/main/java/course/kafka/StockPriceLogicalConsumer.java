package course.kafka;

import course.kafka.model.StockPrice;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class StockPriceLogicalConsumer {
    private int numConsumers;
    private ExecutorService poolExecutor;
    private List<Future<?>> consumers = new ArrayList<>();

    public StockPriceLogicalConsumer(int numConsumers) throws SQLException {
        this.numConsumers = numConsumers;
        poolExecutor = Executors.newFixedThreadPool(numConsumers);
        for(int i = 0; i < numConsumers; i++){
            StockPriceConsumer consumer = new StockPriceConsumer(i);
            consumers.add(poolExecutor.submit(consumer));
        }
    }

    public void shutdown() {
        poolExecutor.shutdownNow();
    }

    public static void main(String[] args) throws SQLException {
        StockPriceLogicalConsumer topConsumer = new StockPriceLogicalConsumer(3);
    }
}
