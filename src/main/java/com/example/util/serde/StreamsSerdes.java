package com.example.util.serde;


import com.example.model.ConnectionEvent;
import com.example.util.serializer.JsonDeserializer;
import com.example.util.serializer.JsonSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;


public class StreamsSerdes {
    public static Serde<ConnectionEvent> ConnectEventSerde() {
        return new ConnectionEventSerde();
    }

    public static final class ConnectionEventSerde extends WrapperSerde<ConnectionEvent> {
        public ConnectionEventSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(ConnectionEvent.class));
        }
    }

    private static class WrapperSerde<T> implements Serde<T> {
        private JsonSerializer<T> serializer;
        private JsonDeserializer<T> deserializer;

         WrapperSerde(JsonSerializer<T> serializer, JsonDeserializer<T> deserializer) {
            this.serializer = serializer;
            this.deserializer = deserializer;
        }

        @Override
        public void configure(Map<String, ?> map, boolean b) {
        }

        @Override
        public void close() {
        }

        @Override
        public Serializer<T> serializer() {
           return serializer;
        }

        @Override
        public Deserializer<T> deserializer() {
           return deserializer;
        }
    }
}
