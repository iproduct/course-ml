package course.dml.serde;

import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import course.dml.exception.JsonSerializationException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;

@Slf4j
public class JsonDeserializer<T> implements Deserializer<T> {
    public static final String KEY_CLASS = "key.class";
    public static final String VALUE_CLASS = "value.class";
    private static final ObjectMapper objectMapper = new ObjectMapper();

    static {
        objectMapper.registerModule(new JavaTimeModule());
    }

    private Class<T> cls;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        String configKey = isKey ? KEY_CLASS: VALUE_CLASS;
        String clsName = String.valueOf(configs.get(configKey));
        try {
            cls = (Class<T>) Class.forName(clsName);
        } catch (ClassNotFoundException e) {
            log.error("Failed to configure JSON Deserializer. " +
                    "Did you forget to specify the '{}' property?", configKey);
            throw new JsonSerializationException("Entity class not found: " + clsName, e);
        }
        Deserializer.super.configure(configs, isKey);
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, cls);
        } catch (IOException e) {
            try {
                var message = new String(data, "utf-8");
                log.error("Error deserializing entity: " + message, e);
                throw new JsonSerializationException("Error deserializing entity: " + message, e);
            } catch (UnsupportedEncodingException ex) {
                new JsonSerializationException("Error decoding data using UTF-8", ex);
            }
            throw new RuntimeException(e);
        }
    }
}
