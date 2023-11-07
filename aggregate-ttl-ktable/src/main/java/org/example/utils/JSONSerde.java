package org.example.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

public class JSONSerde<T> implements Serializer<T>, Deserializer<T>, Serde<T> {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    @Override
    public Serializer<T> serializer() {
        return this;
    }

    @Override
    public Deserializer<T> deserializer() {
        return this;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if(data == null) return null;
        T result;
        try {
            result = (T) OBJECT_MAPPER.readValue(data, JSONSerdeCompatible.class);
            System.out.println("Serialized = "+result);
        } catch (IOException e) {
            throw new SerializationException(e);
        }
        return result;
    }

    @Override
    public byte[] serialize(String topic, T data) {
        if(data == null) return null;
        try {
            return OBJECT_MAPPER.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new SerializationException("Error serializing JSON Message", e);
        }
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }
}
