package course.kafka.serialization;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class JsonSimpleDeserializer implements Deserializer<Map<String,String>> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public Map<String, String> deserialize(String topic, byte[] data) {
        JSONParser parser = new JSONParser();
        try {
            Object result = parser.parse(new String(data));
            if(result instanceof  Map) {
                return (Map<String, String>) result;
            } else {
                log.error("Error parsing JSON data.");
            }
        } catch (ParseException e) {
           log.error("Error parsing JSON data: ", e);
        }
        return new HashMap<>();
    }

    @Override
    public void close() {

    }
}
