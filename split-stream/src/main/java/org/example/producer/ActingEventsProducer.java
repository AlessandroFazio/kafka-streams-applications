package org.example.producer;

import com.example.avro.ActingEvent;
import com.github.javafaker.Faker;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.checkerframework.checker.units.qual.A;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Random;

public class ActingEventsProducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(ActingEventsProducer.class);
    private static final String[] shops =
            {"Melony Street 223", "Marcel Avenue 9900", "Mirror Avenue 111"};
    private static final String[] genres = {"drama", "fantasy", "thriller", "horror"};
    private static final Random RANDOM = new Random();
    private static final Faker FAKER = new Faker();
    private static final String INPUT_TOPIC = "acting-events";

    public static void main(String[] args) {
        if(args.length != 1) {
            throw new IllegalArgumentException("""
                    This program takes exactly 1 argument:
                     - path to producer property file""");
        }
        Properties props = loadProperties(args[0]);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        KafkaProducer<String, ActingEvent> producer = new KafkaProducer<>(props);
        ActingEventsProducer app = new ActingEventsProducer(producer);
        Runtime.getRuntime().addShutdownHook(new Thread(app::shutDown));
        app.produceRecords(INPUT_TOPIC);
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
    private KafkaProducer<String, ActingEvent> producer;
    private boolean closed = false;

    public ActingEventsProducer(KafkaProducer<String, ActingEvent> producer) {
        this.producer = producer;
    }

    public void produceRecords(String inputTopic) {
        try {
            while (!closed) {
                int randomShopIndex = RANDOM.nextInt(0, 2);
                int randomGenreIndex = RANDOM.nextInt(0, 3);
                ActingEvent event = ActingEvent.newBuilder()
                        .setName(FAKER.name().fullName())
                        .setTitle(FAKER.book().title())
                        .setGenre(genres[randomGenreIndex])
                        .build();
                ProducerRecord<String, ActingEvent> producerRecord =
                        new ProducerRecord<>(inputTopic, shops[randomShopIndex], event);

                System.out.println("actingEvent generated: " + event.toString());
                System.out.println("producerRecord generated: " + producerRecord);

                try {
                    producer.send(producerRecord, (metadata, e) -> {
                        if (e != null) {
                            LOGGER.error("Unable to produce Record", e);
                        } else {
                            LOGGER.info("Produced Record -> topic: {}, offset {}, timestamp: {}",
                                    metadata.topic(), metadata.offset(), metadata.timestamp());
                        }
                    });
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    throw new RuntimeException("Producer thread was interrupted", e);
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
            LOGGER.info("Shutting down " + ActingEventsProducer.class.getSimpleName());
            closed = true;
            LOGGER.info("Shut down " + ActingEventsProducer.class.getSimpleName());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
