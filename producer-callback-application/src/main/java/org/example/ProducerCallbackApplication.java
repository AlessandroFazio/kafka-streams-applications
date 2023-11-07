package org.example;

import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ProducerCallbackApplication {

    public static void main(String[] args) throws IOException {
        if(args.length != 1)
            throw new IllegalArgumentException("This program takes exactly one argument:\n "
                    + "- file path to a producer config file");

        String filePath = args[0];
        Properties producerProps = loadProperties(filePath);
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);
        final String outputTopic = producerProps.getProperty("output.topic.name");
        ProducerCallbackApplication app = new ProducerCallbackApplication(producer, outputTopic);

        Thread shutDownThread = new Thread(() -> {
            try {
                Thread.sleep(20000);
                app.shutDown();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        shutDownThread.start();

        System.out.println("Starting " + ProducerCallbackApplication.class.getName());
        app.produce();
    }

    private static Properties loadProperties(String filePath) throws IOException {
        Properties props = new Properties();
        try(InputStream fileInputStream = new FileInputStream(filePath)) {
            props.load(fileInputStream);
        }
        return props;
    }

    private final KafkaProducer<String, String> producer;
    private final String outputTopic;
    private final Faker faker = new Faker();
    private boolean closed = false;

    public ProducerCallbackApplication(
            KafkaProducer<String, String> producer,
            String outputTopic
    ) {
        this.producer = producer;
        this.outputTopic = outputTopic;
    }

    public void produce() {
        while(!closed) {
            try {
                String key = faker.nation().language();
                String value = faker.name().fullName();
                producer.send(
                        new ProducerRecord<>(outputTopic, key, value),
                        (recordMetadata, exception) -> {
                            if(exception == null) {
                                System.out.printf("""
                                                Successfully sent message with:
                                                  - Key: %s
                                                  - Value: %s
                                                  - Timestamp: %s
                                                  - Offset: %s
                                                %n""", key, value, recordMetadata.timestamp(),
                                        recordMetadata.offset());
                            } else {
                                System.err.println("Error: " + exception.getMessage());
                                exception.printStackTrace(System.err);
                            }
                        }
                );
                Thread.sleep(2000);
            } catch (Exception ignored) {
            }
        }
    }

    public void shutDown() {
        closed = true;
        producer.close();
        System.out.println("Shutting down " + ProducerCallbackApplication.class.getName());
    }
}