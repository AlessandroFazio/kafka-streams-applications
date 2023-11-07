package org.example;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.example.avro.Album;
import org.example.avro.MusicInterest;
import org.example.avro.TrackPurchase;
import org.example.joiner.MusicInterestJoiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class JoinOnOForeignTableApplication {
    private static final Logger LOGGER = LoggerFactory.getLogger(JoinOnOForeignTableApplication.class);
    public static void main(String[] args) {
        if(args.length != 2)
            throw new IllegalArgumentException("""
                    This program takes exactly 2 arguments:
                     - a path to the streams config file
                     - a path to the admin config file""");

        Properties streamsProps = loadProperties(args[0]);
        Properties adminProps = loadProperties(args[1]);
        createTopics(adminProps);

        // Add serdeGetter
        JoinOnOForeignTableApplication app = new JoinOnOForeignTableApplication();
        KafkaStreams kafkaStreams =
                new KafkaStreams(app.buildTopology(streamsProps), streamsProps);

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
            LOGGER.error("Un error has occurred", e);
            System.exit(1);
        }
        System.exit(0);
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

    public static void createTopics(Properties props) {
        try (AdminClient client = AdminClient.create(props)) {
            LOGGER.info("Creating topics for " + JoinOnOForeignTableApplication.class.getSimpleName());
            List<NewTopic> topics = new ArrayList<>();

            topics.add(new NewTopic(
                    props.getProperty(props.getProperty("album.topic.name")),
                    Integer.parseInt(props.getProperty("album.topic.partitions")),
                    Short.parseShort(props.getProperty("album.topic.replication.factor"))));

            topics.add(new NewTopic(
                    props.getProperty(props.getProperty("tracks.purchase.topic.name")),
                    Integer.parseInt(props.getProperty("tracks.purchase.topic.partitions")),
                    Short.parseShort(props.getProperty("tracks.purchase.topic.replication.factor"))));

            topics.add(new NewTopic(
                    props.getProperty(props.getProperty("music.interest.topic.name")),
                    Integer.parseInt(props.getProperty("music.interest.topic.partitions")),
                    Short.parseShort(props.getProperty("music.interest.topic.replication.factor"))));

            client.createTopics(topics);
            LOGGER.info("Created topics for " + JoinOnOForeignTableApplication.class.getSimpleName());
        }
    }

    public Topology buildTopology(Properties streamPros) {
        StreamsBuilder builder = new StreamsBuilder();

        String albumInputTopic = streamPros.getProperty("album.topic.name");
        String trackPurchaseInputTopic = streamPros.getProperty("tracks.purchase.topic.name");
        String musicInterestTopic = streamPros.getProperty("music.interest.topic.name");

        SpecificAvroSerde<Album> albumSerde = getSpecificAvroSerde(streamPros, false);
        SpecificAvroSerde<TrackPurchase> trackPurchaseSerde = getSpecificAvroSerde(streamPros, false);
        SpecificAvroSerde<MusicInterest> musicInterestSerde = getSpecificAvroSerde(streamPros, false);

        MusicInterestJoiner joiner = new MusicInterestJoiner();

        KTable<Long, Album> albumTable = builder
                .table(albumInputTopic, Consumed.with(Serdes.Long(), albumSerde));

        KTable<Long, TrackPurchase> trackPurchaseTable = builder
                .table(trackPurchaseInputTopic, Consumed.with(Serdes.Long(), trackPurchaseSerde));

        KTable<Long, MusicInterest> musicInterestKTable = trackPurchaseTable
                .leftJoin(albumTable, TrackPurchase::getAlbumId, joiner);

        musicInterestKTable.toStream()
                .to(musicInterestTopic, Produced.with(Serdes.Long(), musicInterestSerde));

        return builder.build();
    }

    private <T extends SpecificRecord> SpecificAvroSerde<T> getSpecificAvroSerde (
            Properties props, boolean isKey
    ) {
        SpecificAvroSerde<T> specificAvroSerde = new SpecificAvroSerde<>();
        specificAvroSerde.configure(getSerdeConfig(props), isKey);
        return specificAvroSerde;
    }

    private Map<String, String> getSerdeConfig(Properties props) {
        Map<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put("schema.registry.url", props.getProperty("schema.registry.url"));
        return serdeConfig;
    }

    public void shutDown(KafkaStreams kafkaStreams, CountDownLatch latch) {
        LOGGER.info("Shutting down " + JoinOnOForeignTableApplication.class.getSimpleName());
        kafkaStreams.close(Duration.of(5, ChronoUnit.SECONDS));
        latch.countDown();
        LOGGER.info("Shut down " + JoinOnOForeignTableApplication.class.getSimpleName());
    }
}