package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ProducerApplication {
    private final Producer<String, String> producer;
    private final String outTopic;

    public ProducerApplication(
            Producer<String, String> producer,
            String outTopic
    ) {
        this.producer = producer;
        this.outTopic = outTopic;
    }

    public Future<RecordMetadata> produce(String message) {
        String[] parts = message.split(":");
        final String key, value;
        if(parts.length > 1) {
            key = parts[0];
            value = parts[1];
        } else {
            key = "NO-KEY";
            value = parts[0];
        }

        final ProducerRecord<String, String> record =
                new ProducerRecord<>(outTopic, key, value);
        return producer.send(record);
    }

    public void shutDown() {
        producer.close();
    }

    public static Properties loadProperties(String fileName) throws FileNotFoundException {
        Properties envProps = new Properties();
        try(final InputStream fileInput = new FileInputStream(fileName)) {
            envProps.load(fileInput);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return envProps;
    }

    public void printMetadata(
            Collection<Future<RecordMetadata>> metadata,
            String fileName
    ) {
        System.out.println("Offsets and timestamps committed in batch from " + fileName);
        metadata.forEach(m -> {
            try {
                final RecordMetadata recordMetadata = m.get();
                System.out.println("Committed record with:\n  Offset: "
                        + recordMetadata.offset()
                        + "\n  Timestamp: " + recordMetadata.timestamp());

            } catch (InterruptedException | ExecutionException e) {
                if(e instanceof InterruptedException)
                    Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        });
    }

    public static void main(String[] args) throws FileNotFoundException {
        if(args.length > 2) {
            throw new IllegalArgumentException("This program (%d) takes exactly 2 arguments:\n  - path to env file\n  - path to file with record to send");
        }

        final Properties envProps = loadProperties(args[0]);
        final String topic = envProps.getProperty("output.topic.name");
        final Producer<String, String> producer = new KafkaProducer<>(envProps);
        final ProducerApplication app = new ProducerApplication(producer, topic);

        String filePath = args[1];
        try {
            List<String> lines = Files.readAllLines(Paths.get(filePath));
            List<Future<RecordMetadata>> metadata = lines.stream()
                    .filter(l -> !l.trim().isEmpty())
                    .map(app::produce)
                    .toList();

            app.printMetadata(metadata, filePath);
        } catch (IOException e) {
            System.err.printf("Error reading file %s due to %s %n", filePath, e);
        }
        finally {
            app.shutDown();
        }
    }
}
