package rassus.lab2;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.json.JSONObject;

import java.util.Map;

public class CustomJSONSerializer implements Serializer<JSONObject> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Serializer.super.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String topic, JSONObject data) {
        try {
            if(data == null) {
                System.out.println("Null received at serializing");
                return null;
            }
            System.out.println("Serializing...");
            return objectMapper.writeValueAsBytes(data);
        }
        catch (Exception e) {
            throw new SerializationException("Error when serializing JSONObject to byte[]");
        }
    }

    @Override
    public void close() {
        Serializer.super.close();
    }
}
