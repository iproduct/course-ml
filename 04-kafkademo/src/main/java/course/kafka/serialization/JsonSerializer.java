package course.kafka.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

@Slf4j
public class JsonSerializer<T> implements Serializer<T> {
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, T data) {
        try {
            String json = mapper.writeValueAsString(data);
            log.debug(">>>JSON: {}", json);
            return json.getBytes();
        } catch (JsonProcessingException e) {
            log.error("Error serializing object to JSON: ", e);
        }
        return "".getBytes();
    }

    @Override
    public void close() {

    }
}
