package course.kafka.listener;

import course.kafka.dao.PricesDAO;
import course.kafka.model.StockPrice;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

@Slf4j
public class StockPriceRebalanceListener implements ConsumerRebalanceListener {
    Consumer<String, StockPrice> consumer;
    PricesDAO dao;
    String consumerGroupId;

    public StockPriceRebalanceListener(Consumer<String, StockPrice> consumer,
                                       PricesDAO dao,
                                       String consumerGroupId
    ) {
        this.consumer = consumer;
        this.dao = dao;
        this.consumerGroupId = consumerGroupId;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
//        consumer.seekToBeginning(partitions);
        try {
            Map<TopicPartition, OffsetAndMetadata> savedOffsets = dao.getOffsetsByConsumerGroupId(consumerGroupId);
            for(TopicPartition tp: partitions) {
                var offset = savedOffsets.getOrDefault(tp, new OffsetAndMetadata(0));
                consumer.seek(tp, offset);
                log.info("Seek offset for partition: {} --> {}", tp, offset);
            }
        } catch (SQLException e) {
            log.error("Error fetching Consumer Group offsets from database.", e);
        }
    }
}
