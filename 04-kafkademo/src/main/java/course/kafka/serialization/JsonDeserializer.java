package course.kafka.serialization;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import course.kafka.model.Customer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

@Slf4j
public class JsonDeserializer<T> implements Deserializer<T> {
    public static final String CONFIG_VALUE_CLASS = "value.deserializer.class";
    public static final String CONFIG_KEY_CLASS = "key.deserializer.class";
    private final ObjectMapper mapper = new ObjectMapper();
    private Class<T> cls;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        String configKey = isKey ? CONFIG_KEY_CLASS : CONFIG_VALUE_CLASS;
        String clsName = String.valueOf(configs.get(configKey));
        try {
            cls = (Class<T>) Class.forName(clsName);
        } catch (ClassNotFoundException e) {
            log.error("Failed to configure JsonDeserializer. " +
                    "Did you forget to specify the '{}' property?", configKey);
        }
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        try {
            return mapper.readValue(data, cls);
        } catch (IOException e) {
            log.error("Error deserializing object from JSON: ", e);
        }
        return null;
    }

    @Override
    public void close() {

    }
}
