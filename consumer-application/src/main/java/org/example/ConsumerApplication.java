package org.example;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.example.handler.ConsumerRecordsHandler;
import org.example.handler.RecordsHandler;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

public class ConsumerApplication {
    public static void main(String[] args) throws IOException, InterruptedException {
        if(args.length != 1)
            throw new IllegalArgumentException("This program expects exactly 1 argument: path for properties file. Got "
                    + args.length + " instead.");

        Properties envProps = loadProperties(args[0]);
        Consumer<String, String> consumer = new KafkaConsumer<>(envProps);
        RecordsHandler<String, String> handler = new ConsumerRecordsHandler<>();
        ConsumerApplication app = new ConsumerApplication(consumer, handler);

        Runtime.getRuntime().addShutdownHook(new Thread(app::shutDown));
        app.runConsume(envProps);
    }

    private final Consumer<String, String> consumer;
    private final RecordsHandler<String, String> recordsHandler;
    private volatile boolean keepConsuming = true;

    public ConsumerApplication(
            Consumer<String, String> consumer,
            RecordsHandler<String, String> recordsHandler
    ) {
        this.consumer = consumer;
        this.recordsHandler = recordsHandler;
    }

    public void runConsume(final Properties envProps) {
        try {
            Collection<String> topics = Collections.singletonList(envProps.getProperty("input.topic.name"));
            consumer.subscribe(topics);
            while(keepConsuming) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.of(5, ChronoUnit.SECONDS));
                recordsHandler.handle(records);
            }
        } catch (Exception ignored) {
        } finally {
            consumer.close();
        }
    }

    public void shutDown() {
        keepConsuming = false;
    }

    private static Properties loadProperties(String fileName) throws IOException {
        Properties envProps = new Properties();
        try(InputStream fileInputStream = new FileInputStream(fileName)) {
            envProps.load(fileInputStream);
        }
        return envProps;
    }
}