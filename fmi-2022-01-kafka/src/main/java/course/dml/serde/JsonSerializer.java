package course.dml.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import course.dml.exception.JsonSerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class JsonSerializer<T> implements Serializer<T> {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    static {
        objectMapper.registerModule(new JavaTimeModule());
    }


    @Override
    public byte[] serialize(String topic, T entity) {
        try {
            return objectMapper.writeValueAsBytes(entity);
        } catch (JsonProcessingException e) {
            throw new JsonSerializationException("Error serializing entity to JSON: " + entity, e);
        }
    }
}
