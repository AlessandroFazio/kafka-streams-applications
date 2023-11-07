package org.example;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.example.avro.SongEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class MergeStreamsApplication {
    private static final Logger LOGGER = LoggerFactory.getLogger(MergeStreamsApplication.class);
    public static void main(String[] args) {
        if(args.length != 2) {
            throw new IllegalArgumentException("""
                    This program takes exactly 2 arguments:
                     - a path to the streams config file
                     - a path to the admin config file""");
        }
        LOGGER.info("Starting " + MergeStreamsApplication.class.getSimpleName());

        Properties streamsProps = loadProperties(args[0]);
        streamsProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

        Properties adminProps = loadProperties(args[1]);

        MergeStreamsApplication app = new MergeStreamsApplication();
        app.createTopics(adminProps);

        KafkaStreams kafkaStreams = new KafkaStreams(app.buildTopology(streamsProps), streamsProps);

        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook"){
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
            throw new RuntimeException(e);
        } finally {
            kafkaStreams.close();
        }
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

    public Topology buildTopology(Properties props) {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, SongEvent> rockStream = builder.stream(props.getProperty("input.rock.topic.name"));
        KStream<String, SongEvent> jazzStream = builder.stream(props.getProperty("input.jazz.topic.name"));
        KStream<String, SongEvent> popStream = builder.stream(props.getProperty("input.pop.topic.name"));

        KStream<String, SongEvent> allSongsStream = rockStream.merge(jazzStream).merge(popStream);
        allSongsStream.to(props.getProperty("output.all-songs.topic.name"));
        return builder.build();
    }

    public void createTopics(Properties props) {
        try(AdminClient client = AdminClient.create(props)) {
            LOGGER.info("Creating topics for " + MergeStreamsApplication.class.getSimpleName());
            List<NewTopic> topics = new ArrayList<>();

            topics.add(new NewTopic(
                    props.getProperty(props.getProperty("input.rock.topic.name")),
                    Integer.parseInt(props.getProperty("input.rock.topic.partitions")),
                    Short.parseShort(props.getProperty("input.rock.topic.replication.factor"))));

            topics.add(new NewTopic(
                    props.getProperty(props.getProperty("input.jazz.topic.name")),
                    Integer.parseInt(props.getProperty("input.jazz.topic.partitions")),
                    Short.parseShort(props.getProperty("input.jazz.topic.replication.factor"))));

            topics.add(new NewTopic(
                    props.getProperty(props.getProperty("input.pop.topic.name")),
                    Integer.parseInt(props.getProperty("input.pop.topic.partitions")),
                    Short.parseShort(props.getProperty("input.pop.topic.replication.factor"))));

            topics.add(new NewTopic(
                    props.getProperty(props.getProperty("output.all-songs.topic.name")),
                    Integer.parseInt(props.getProperty("output.all-songs.topic.partitions")),
                    Short.parseShort(props.getProperty("output.all-songs.topic.replication.factor"))));

            client.createTopics(topics);
            LOGGER.info("Created topics for " + MergeStreamsApplication.class.getSimpleName());
        }
    }

    public void shutDown(KafkaStreams kafkaStreams, CountDownLatch latch) {
        LOGGER.info("Shutting down " + MergeStreamsApplication.class.getSimpleName());
        kafkaStreams.close(Duration.of(5, ChronoUnit.SECONDS));
        latch.countDown();
        LOGGER.info("Shut down " + MergeStreamsApplication.class.getSimpleName());
    }
}