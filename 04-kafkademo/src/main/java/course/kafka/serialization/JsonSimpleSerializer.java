package course.kafka.serialization;

import org.apache.kafka.common.serialization.Serializer;
import org.json.simple.JSONObject;

import java.util.Map;

public class JsonSimpleSerializer implements Serializer<Map<String,String>> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, Map<String, String> data) {
        JSONObject json = new JSONObject(data);
        return json.toJSONString().getBytes();
    }

    @Override
    public void close() {

    }
}
