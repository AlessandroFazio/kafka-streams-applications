package org.example.producer;

import com.github.javafaker.Faker;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.avro.ClickEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class ClickEventsProducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClickEventsProducer.class);
    private static final int num_users = 10;
    private static final UUID[] users = new UUID[num_users];

    static { for (int i = 0; i < num_users; i++) users[i] = UUID.randomUUID(); }
    private static final Random RANDOM = new Random();
    private static final Faker FAKER = new Faker();
    private static final String INPUT_TOPIC = "click-events";

    public static void main(String[] args) {
        if(args.length != 1) {
            throw new IllegalArgumentException("""
                    This program takes exactly 1 argument:
                     - path to producer property file""");
        }
        Properties props = loadProperties(args[0]);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        KafkaProducer<String, ClickEvent> producer = new KafkaProducer<>(props);
        ClickEventsProducer app = new ClickEventsProducer(producer);
        Runtime.getRuntime().addShutdownHook(new Thread(app::shutDown));
        app.produceRecords();
    }

    private static Properties loadProperties(String fileName) {
        Properties props = new Properties();
        try(InputStream fileInputStream = new FileInputStream(fileName)) {
            props.load(fileInputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return props;
    }
    private KafkaProducer<String, ClickEvent> producer;
    private boolean closed = false;

    public ClickEventsProducer(KafkaProducer<String, ClickEvent> producer) {
        this.producer = producer;
    }

    public void produceRecords() {
        try {
            while(!closed) {
                int randomUserIndex = RANDOM.nextInt(0, num_users);
                ClickEvent event = ClickEvent.newBuilder()
                        .setUrl(FAKER.internet().url())
                        .setIp(FAKER.internet().ipV4Address())
                        .setTimestamp(String.valueOf(System.currentTimeMillis()))
                        .build();

                ProducerRecord<String, ClickEvent> record = new ProducerRecord<>(
                        INPUT_TOPIC, users[randomUserIndex].toString(), event);

                try {
                    producer.send(record, ((metadata, e) -> {
                        if(e != null) {
                            LOGGER.error("Unable to produce record for topic {}", INPUT_TOPIC);
                        } else {
                            LOGGER.info("Produced Record -> topic: {}, offset {}, timestamp: {}",
                                    metadata.topic(), metadata.offset(), metadata.timestamp());
                        }
                    }));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            producer.flush();
            producer.close();
        }
    }

    public void shutDown() {
        try {
            LOGGER.info("Shutting down " + ClickEventsProducer.class.getSimpleName());
            closed = true;
            LOGGER.info("Shut down " + ClickEventsProducer.class.getSimpleName());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
