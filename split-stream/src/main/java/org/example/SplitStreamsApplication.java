package org.example;

import com.example.avro.ActingEvent;
import com.fasterxml.jackson.databind.annotation.JsonAppend;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.eclipse.jetty.util.IO;
import org.example.producer.ActingEventsProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CountDownLatch;

import static java.util.Collections.singletonMap;

public class SplitStreamsApplication {
    private static Logger LOGGER = LoggerFactory.getLogger(SplitStreamsApplication.class);
    public static void main(String[] args) {
        if(args.length != 2)
            throw new IllegalArgumentException("""
                    This program takes exactly 2 arguments:
                     - a path to the streamProps file
                     - a path to the adminProps file
                    """);
        LOGGER.info("Starting " + SplitStreamsApplication.class.getSimpleName());

        Properties streamProps = loadProperties(args[0]);
        streamProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

        Properties adminProps = loadProperties(args[1]);

        SplitStreamsApplication app = new SplitStreamsApplication();
        app.createTopics(adminProps);

        KafkaStreams kafkaStreams = new KafkaStreams(app.buildTopology(streamProps), streamProps);
        CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread ("streams-shutdown-hook"){
            @Override
            public void run() {
                app.shutDown(kafkaStreams, latch);
            }
        });

        try {
            kafkaStreams.cleanUp();
            kafkaStreams.start();
            latch.await();
        } catch (Exception e) {
            LOGGER.error("Unable to start KafkaStream", e);
            throw new RuntimeException(e);
        }
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

    public Topology buildTopology(Properties streamProps) {
        String inputTopic = streamProps.getProperty("input.topic.name");
        StreamsBuilder builder = new StreamsBuilder();

        builder.<String, ActingEvent> stream(inputTopic)
                .split()
                .branch(
                        (key, appearance) -> "drama".contentEquals(appearance.getGenre()),
                        Branched.withConsumer(ks -> ks.to(streamProps.getProperty("output.drama.topic.name"))))
                .branch(
                        (key, appearance) -> "fantasy".contentEquals(appearance.getGenre()),
                        Branched.withConsumer(ks -> ks.to(streamProps.getProperty("output.fantasy.topic.name"))))
                .branch(
                        (key, appearance) -> true,
                        Branched.withConsumer(ks -> ks.to(streamProps.getProperty("output.other.topic.name"))));

        return builder.build();
    }

    public void createTopics(Properties adminProps) {
        try(AdminClient client = AdminClient.create(adminProps)) {
            List<NewTopic> topics = new ArrayList<>();

            /* topics.add(new NewTopic(
                    adminProps.getProperty("input.topic.name"),
                    Integer.parseInt(adminProps.getProperty("input.topic.partitions")),
                    Short.parseShort(adminProps.getProperty("input.topic.replication.factor")))); */

            topics.add(new NewTopic(
                    adminProps.getProperty("output.drama.topic.name"),
                    Integer.parseInt(adminProps.getProperty("output.drama.topic.partitions")),
                    Short.parseShort(adminProps.getProperty("output.drama.topic.replication.factor"))));

            topics.add(new NewTopic(
                    adminProps.getProperty("output.fantasy.topic.name"),
                    Integer.parseInt(adminProps.getProperty("output.fantasy.topic.partitions")),
                    Short.parseShort(adminProps.getProperty("output.fantasy.topic.replication.factor"))));

            topics.add(new NewTopic(
                    adminProps.getProperty("output.other.topic.name"),
                    Integer.parseInt(adminProps.getProperty("output.other.topic.partitions")),
                    Short.parseShort(adminProps.getProperty("output.other.topic.replication.factor"))));

            client.createTopics(topics);
            topics.forEach(topic -> LOGGER.info("Created topic {}", topic));
        }
    }

    public void shutDown(KafkaStreams kafkaStreams, CountDownLatch latch) {
        LOGGER.info("Shutting down the " + SplitStreamsApplication.class.getSimpleName());
        kafkaStreams.close(Duration.of(5, ChronoUnit.SECONDS));
        latch.countDown();
    }
}