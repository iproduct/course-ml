package course.kafka.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import course.kafka.model.Customer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

@Slf4j
public class CustomerDeserializer implements Deserializer<Customer> {
    private final ObjectMapper mapper = new ObjectMapper();
//    TypeReference<T> typeReference;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
//        typeReference = (TypeReference<T>) configs.get("targetType");
    }

    @Override
    public Customer deserialize(String topic, byte[] data) {
        try {
            return mapper.readValue(data, Customer.class);
        } catch (IOException e) {
            log.error("Error deserializing object from JSON: ", e);
        }
        return new Customer();
    }

    @Override
    public void close() {

    }
}
