package org.example;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.example.utils.Utility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class StreamsApplication {
    private static final Logger logger = LoggerFactory.getLogger(StreamsApplication.class);

    public static void main(String[] args) throws Exception {
        if (args.length != 2)
            throw new IllegalArgumentException("""
                    This program takes exactly 2 arguments:
                     -1: path to a streams config file
                     -2: path to a producer config file\s""");

        Properties streamsProps = new Properties();
        try(final InputStream fileInputStream = new FileInputStream(args[0])) {
            streamsProps.load(fileInputStream);
        }

        Properties producerProps = new Properties();
        try(final InputStream fileInputStream = new FileInputStream(args[1])) {
            producerProps.load(fileInputStream);
        }

        final String inputTopic = streamsProps.getProperty("input.topic.name");
        final String outputTopic = streamsProps.getProperty("output.topic.name");

        try(Utility utility = new Utility()) {
            utility.createTopics(
                    streamsProps,
                    Arrays.asList(
                            new NewTopic(inputTopic, Optional.empty(), Optional.empty()),
                            new NewTopic(outputTopic, Optional.empty(), Optional.empty())));

            try(Utility.Randomizer random = utility.newRandomizer(producerProps, inputTopic)) {
                KafkaStreams kafkaStreams = new KafkaStreams(
                        buildTopology(inputTopic, outputTopic),
                        streamsProps);

                Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
                logger.info("Kafka Streams 101 App Started");
                runKafkaStreams(kafkaStreams);
            }
        }
    }

    public static Topology buildTopology(String inputTopic, String outputTopic) {
        Serde<String> stringSerde = Serdes.String();
        StreamsBuilder builder = new StreamsBuilder();

        builder.stream(inputTopic, Consumed.with(stringSerde, stringSerde))
                .peek((k,v) -> logger.info("Observed event: {}", v))
                .mapValues(v -> v.toUpperCase())
                .peek((k,v) -> logger.info("Transformed event: {}", v))
                .to(outputTopic, Produced.with(stringSerde, stringSerde));

        return builder.build();
    }

    public static void runKafkaStreams(final KafkaStreams kafkaStreams) {
        CountDownLatch latch = new CountDownLatch(1);
        kafkaStreams.setStateListener((oldState, newState) -> {
            if (oldState == KafkaStreams.State.RUNNING && newState != KafkaStreams.State.RUNNING)
                latch.countDown();
        });
        kafkaStreams.start();

        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        logger.info("Streams Closed");
    }
}