package course.kafka;

import course.kafka.dao.PricesDAO;
import course.kafka.model.StockPrice;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import static course.kafka.StockPriceConstants.PRICES_TOPIC;

@Slf4j
public class StockPriceRebalanceListener  implements ConsumerRebalanceListener {
    KafkaConsumer<String, StockPrice> consumer;
    PricesDAO dao;
    String consumerGroupId;

    public StockPriceRebalanceListener(KafkaConsumer<String, StockPrice> consumer,
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
                consumer.seek(tp, savedOffsets.get(tp));
                log.debug("Seek offset for partition: {} --> {}", tp, savedOffsets.get(tp));
            }
        } catch (SQLException e) {
            log.error("Error fetching Consumer Group offsets from database.", e);
        }
    }
}
