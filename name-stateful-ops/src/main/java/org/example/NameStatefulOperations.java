package org.example;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
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

public class NameStatefulOperations {
    private static Logger LOGGER = LoggerFactory.getLogger(NameStatefulOperations.class);

    public static void main(String[] args) throws IOException {
        if(args.length != 2)
            throw new IllegalArgumentException("""
                    This program takes exactly 2 arguments:
                     - path to streams properties file
                     - path to admin properties file""");

        Properties streamsProps = loadProperties(args[0]);
        streamsProps.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        Properties adminProps = loadProperties(args[1]);

        createTopics(adminProps);
        NameStatefulOperations app = new NameStatefulOperations();
        Topology topology = app.buildTopology(streamsProps);
        KafkaStreams kafkaStreams = new KafkaStreams(topology, streamsProps);
        CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook"){
            @Override
            public void run() {
                app.shutDown(kafkaStreams, latch);
            }
        });
        LOGGER.info("Topology: {}", topology.describe().toString());

        try {
            kafkaStreams.cleanUp();
            kafkaStreams.start();
            latch.await();
        } catch (Exception e) {
            LOGGER.error("Kafka Streams got an error: ", e);
            throw new RuntimeException(e);
        } finally {
            kafkaStreams.close();
        }
        System.exit(0);
    }

    public static void createTopics(Properties adminProps) {
        try(AdminClient client = AdminClient.create(adminProps)) {
            List<NewTopic> topics = new ArrayList<>();

            topics.add(new NewTopic(
                    adminProps.getProperty("input.topic.name"),
                    Integer.parseInt(adminProps.getProperty("input.topic.partitions")),
                    Short.parseShort(adminProps.getProperty("input.topic.replication.factor"))));

            topics.add(new NewTopic(
                    adminProps.getProperty("output.topic.name"),
                    Integer.parseInt(adminProps.getProperty("output.topic.partitions")),
                    Short.parseShort(adminProps.getProperty("output.topic.replication.factor"))));

            topics.add(new NewTopic(
                    adminProps.getProperty("join.topic.name"),
                    Integer.parseInt(adminProps.getProperty("join.topic.partitions")),
                    Short.parseShort(adminProps.getProperty("join.topic.replication.factor"))));

            LOGGER.info("Creating topics");
            client.createTopics(topics);
            LOGGER.info("Created topics");
        }
    }

    public static Properties loadProperties(String fileName) throws IOException {
        Properties props = new Properties();
        try(FileInputStream fileInputStream = new FileInputStream(fileName)) {
            props.load(fileInputStream);
        }
        return props;
    }
    public Topology buildTopology(Properties streamsProps) {
        StreamsBuilder builder = new StreamsBuilder();

        String inputTopic = streamsProps.getProperty("input.topic.name");
        String outputTopic = streamsProps.getProperty("output.topic.name");
        String joinTopic = streamsProps.getProperty("join.topic.name");

        Serde<String> stringSerde = Serdes.String();
        Serde<Long> longSerde = Serdes.Long();

        boolean addFilter = Boolean.parseBoolean(streamsProps.getProperty("add.filter"));
        boolean nameFilter = Boolean.parseBoolean(streamsProps.getProperty("name.filter"));

        KStream<Long, String> inputStream = builder
                .stream(inputTopic, Consumed.with(longSerde, stringSerde))
                .selectKey((k,v) -> Long.parseLong(v.substring(0,1)))
                .filter((k,v) -> k != 1L);

        if(addFilter) inputStream.filter((k,v) -> k != 100L);

        KStream<Long, Long> countStream;
        KStream<Long, String> joinStream;

        if(!nameFilter) {
            countStream = inputStream
                    .groupByKey(Grouped.with(longSerde, stringSerde))
                    .count()
                    .toStream();

            joinStream = inputStream.join(countStream, (v1,v2) -> v1 + v2.toString(),
                    JoinWindows.ofTimeDifferenceWithNoGrace(Duration.of(100, ChronoUnit.MILLIS)),
                    StreamJoined.with(longSerde, stringSerde, longSerde));
        } else {
            countStream = inputStream
                    .groupByKey(Grouped.with(longSerde, stringSerde))
                    .count(Materialized.as("the-counting-store"))
                    .toStream();

            joinStream = inputStream.join(countStream, (v1,v2) -> v1 + v2.toString(),
                    JoinWindows.ofTimeDifferenceWithNoGrace(Duration.of(100, ChronoUnit.MILLIS)),
                    StreamJoined.with(longSerde, stringSerde, longSerde)
                            .withName("join").withStoreName("the-join-store"));
        }
        joinStream.to(joinTopic, Produced.with(longSerde, stringSerde));
        countStream.map((k,v) -> KeyValue.pair(k.toString(), v.toString()))
                .to(outputTopic, Produced.with(stringSerde, stringSerde));

        return builder.build();
    }
    public void shutDown(KafkaStreams kafkaStreams, CountDownLatch latch) {
        LOGGER.info("Shutting down " + NameStatefulOperations.class.getSimpleName());
        kafkaStreams.close(Duration.of(5, ChronoUnit.SECONDS));
        latch.countDown();
        LOGGER.info("Shut down "+ NameStatefulOperations.class.getSimpleName());
    }
}