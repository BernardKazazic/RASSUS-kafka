package rassus.lab2;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.json.JSONObject;

import java.util.Map;

public class CustomJSONDeserializer implements Deserializer<JSONObject> {
    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Deserializer.super.configure(configs, isKey);
    }

    @Override
    public JSONObject deserialize(String topic, byte[] data) {
        try {
            if (data == null){
                System.out.println("Null received at deserializing");
                return null;
            }
            System.out.println("Deserializing...");
            return objectMapper.readValue(new String(data, "UTF-8"), JSONObject.class);
        } catch (Exception e) {
            throw new SerializationException("Error when deserializing byte[] to JSONObject");
        }
    }

    @Override
    public void close() {
        Deserializer.super.close();
    }
}
