package org.example;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.example.handler.MaxFailuresUncaughtExceptionHandler;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class StreamsApplicationErrorHandling {
    public static void main(String[] args) throws IOException {
        if(args.length != 1)
            throw new IllegalArgumentException("This program takes exactly one argument: path to streams app config file");

        String filePath = args[0];
        Properties streamsProps = loadProperties(filePath);
        StreamsApplicationErrorHandling app = new StreamsApplicationErrorHandling();
        String inputTopic = streamsProps.getProperty("input.topic.name");
        String outputTopic = streamsProps.getProperty("output.topic.name");
        app.createTopicsIfNotExist(streamsProps, Arrays.asList(inputTopic, outputTopic));

        Topology topology = app.buildTopology(inputTopic, outputTopic);
        KafkaStreams kafkaStreams = new KafkaStreams(topology, streamsProps);

        final StreamsUncaughtExceptionHandler exceptionHandler =
                new MaxFailuresUncaughtExceptionHandler(
                        Integer.parseInt(streamsProps.getProperty("max.failures")),
                        Long.parseLong(streamsProps.getProperty("max.time.millis")));

        kafkaStreams.setUncaughtExceptionHandler(exceptionHandler);

        Thread shutDownThread = new Thread(() -> {
            try {
                Thread.sleep(30000);
                stopKafkaStreams(kafkaStreams);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() ->
                kafkaStreams.close(Duration.of(3, ChronoUnit.SECONDS))));

        runKafkaStreams(kafkaStreams);
        shutDownThread.start();
    }

    private static Properties loadProperties(String filePath) throws IOException {
        Properties props = new Properties();
        try(InputStream fileInputStream = new FileInputStream(filePath)) {
            props.load(fileInputStream);
        }
        return props;
    }

    public static void runKafkaStreams(KafkaStreams kafkaStreams) {
        System.out.println("Starting KafkaStreams for " + StreamsApplicationErrorHandling.class.getName());
        try {
            kafkaStreams.cleanUp();
            kafkaStreams.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void stopKafkaStreams(KafkaStreams kafkaStreams) {
        System.out.println("Shutting down KafkaStreams for " + StreamsApplicationErrorHandling.class.getName());
        kafkaStreams.close();
    }

    private int count;

    private void createTopics(Properties props, List<String> topics) {
        try(final AdminClient client = AdminClient.create(props)) {
            client.createTopics(
                    topics.stream()
                            .map(topic -> new NewTopic(topic, Optional.empty(), Optional.empty()))
                            .toList())
                    .values()
                    .forEach((topicName, future) -> {
                        try {
                            future.get();
                            System.out.println("Created topic " + topicName);
                        } catch (Exception e) {
                            System.out.println("Error creating topic " + topicName);
                        }
                    });
        }
    }

    public List<String> topicsToCreate(Properties props, List<String> topics) {
        try(AdminClient client = AdminClient.create(props)) {
            ListTopicsResult allTopics = client.listTopics();
            Set<String> topicNames;

            try {
                topicNames = allTopics.names().get();
            } catch (Exception e) {
                System.out.println("Error listing topics with AdminClient");
                throw new RuntimeException(e);
            }
            return topicNames.stream()
                    .filter(topics::contains)
                    .toList();
        }
    }

    public void createTopicsIfNotExist(Properties props, List<String> topics) {
        List<String> topicsToCreate = topicsToCreate(props, topics);
        if(topicsToCreate != null) {
            createTopics(props, topicsToCreate);
        }
    }

    public Topology buildTopology(String inputTopic, String outputTopic) {
        StreamsBuilder builder = new StreamsBuilder();
        builder
                .stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .peek((k,v) -> System.out.printf("""
                                Peeked record with:
                                 - Key: %s
                                 - Value: %s
                                %n""", k, v))
                .mapValues(v -> {
                    count++;
                    if(count % 5 == 0)
                        throw new IllegalStateException("This is intentional!");
                    return v.toUpperCase();
                })
                .peek((k,v) -> System.out.printf("""
                        Transformed record to:
                          - Key: %s
                          - Value: %s
                          %n""", k, v))
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();
    }
}