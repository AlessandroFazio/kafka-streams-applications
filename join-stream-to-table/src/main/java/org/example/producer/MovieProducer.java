package org.example.producer;

import com.fasterxml.jackson.databind.ser.std.StringSerializer;
import com.github.javafaker.Faker;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.avro.Movie;
import org.example.avro.Rating;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;

public class MovieProducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(RatingProducer.class);
    static String TOPIC = "movies";

    public static void main(String[] args) {
        if(args.length != 1) {
            throw new IllegalArgumentException("""
                    This program takes exactly 1 argument:
                     - path to producer property file""");
        }
        Properties props = loadProperties(args[0]);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        KafkaProducer<String, Rating> producer = new KafkaProducer<>(props);
        RatingProducer app = new RatingProducer(producer);
        Runtime.getRuntime().addShutdownHook(new Thread(app::shutDown));
    }

    private static Properties loadProperties(String fileName) {
        Properties props = new Properties();
        try(FileInputStream fileInputStream = new FileInputStream(fileName)) {
            props.load(fileInputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return props;
    }
    private final KafkaProducer<String, Movie> producer;
    private boolean closed = false;

    public MovieProducer(KafkaProducer<String, Movie> producer) {
        this.producer = producer;
    }

    public void produceRecords(long[] uuidPool, String[] movieTitles, int[] years) {
        try {
            int i = 0;
            while(!closed) {
                int index = i++ % uuidPool.length;
                Movie event = Movie.newBuilder()
                        .setId(uuidPool[index])
                        .setTitle(movieTitles[index])
                        .setReleaseYear(years[index])
                        .build();

                ProducerRecord<String, Movie> record = new ProducerRecord<>(
                        TOPIC, "", event);

                try {
                    producer.send(record, ((metadata, e) -> {
                        if(e != null) {
                            LOGGER.error("Unable to produce record for topic {}", TOPIC);
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
            LOGGER.info("Shutting down " + RatingProducer.class.getSimpleName());
            closed = true;
            LOGGER.info("Shut down " + RatingProducer.class.getSimpleName());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
