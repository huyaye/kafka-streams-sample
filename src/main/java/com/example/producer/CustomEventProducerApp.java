package com.example.producer;

import com.example.config.HTConstant;
import com.example.model.ConnectionEvent;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class CustomEventProducerApp {
    private final static Logger logger = LoggerFactory.getLogger(CustomEventProducerApp.class);
    private final static String TOPIC_NAME = HTConstant.SOURCE_TOPIC;
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private static final Gson gson = new GsonBuilder().disableHtmlEscaping().create();

    public static void main(String[] args) {
        Properties configs = new Properties();
        // 필수 옵션
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 선택 옵션
//        configs.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class);

        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

        List<ConnectionEvent> eventList = getEventData(1, 200);
        for (ConnectionEvent event : eventList) {
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, event.getSerial(), convertToJson(event));
            producer.send(record, new ProducerCallback());
        }
//        producer.flush();
        producer.close();
    }

    private static <T> String convertToJson(T generatedDataItem) {
        return gson.toJson(generatedDataItem);
    }

    private static List<ConnectionEvent> getEventData(int start, int end) {
        List<ConnectionEvent> list = new ArrayList<>();
        for (int i = start; i <= end; i++) {
            list.add(new ConnectionEvent("S-" + new Random().nextInt(10), "on", "IP-" + new Random().nextInt(10)));
        }
        return list;
    }
}