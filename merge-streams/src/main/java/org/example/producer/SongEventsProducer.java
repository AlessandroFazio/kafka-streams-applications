package org.example.producer;

import com.github.javafaker.Faker;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.avro.SongEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Random;

public class SongEventsProducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(SongEventsProducer.class);

    private static final String[] shops =
            {"Melony Street 223", "Marcel Avenue 9900", "Mirror Avenue 111"};
    private static final String[] genres = {"rock", "jazz", "pop"};
    private static final Random RANDOM = new Random();
    private static final Faker FAKER = new Faker();
    private static final String[] INPUT_TOPICS =
            {"rock-songs-events", "jazz-songs-events", "pop-songs-events"};

    public static void main(String[] args) {
        if(args.length != 1) {
            throw new IllegalArgumentException("""
                    This program takes exactly 1 argument:
                     - path to producer property file""");
        }
        Properties props = loadProperties(args[0]);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        KafkaProducer<String, SongEvent> producer = new KafkaProducer<>(props);
        SongEventsProducer app = new SongEventsProducer(producer);
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
    private KafkaProducer<String, SongEvent> producer;
    private boolean closed = false;

    public SongEventsProducer(KafkaProducer<String, SongEvent> producer) {
        this.producer = producer;
    }

    public void produceRecords() {
        try {
            while(!closed) {
                int randomShopIndex = RANDOM.nextInt(0,3);
                int randomGenreIndex = RANDOM.nextInt(0,3);
                String inputTopic = INPUT_TOPICS[randomGenreIndex];
                SongEvent event = SongEvent.newBuilder()
                        .setArtist(FAKER.name().fullName())
                        .setTitle(FAKER.book().title())
                        .build();

                ProducerRecord<String, SongEvent> record = new ProducerRecord<>(
                        inputTopic, shops[randomShopIndex], event);

                try {
                    producer.send(record, ((metadata, e) -> {
                        if(e != null) {
                            LOGGER.error("Unable to produce record for topic {}", inputTopic);
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
            LOGGER.info("Shutting down " + SongEventsProducer.class.getSimpleName());
            closed = true;
            LOGGER.info("Shut down " + SongEventsProducer.class.getSimpleName());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
