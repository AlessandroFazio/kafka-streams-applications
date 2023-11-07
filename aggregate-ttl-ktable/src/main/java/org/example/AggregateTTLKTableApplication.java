package org.example;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.example.transformer.TTLKTableTombstoneEmitter;
import org.example.utils.JSONSerde;
import org.example.wrapper.AggregateObject;
import org.example.wrapper.ValueWrapper;
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

public class AggregateTTLKTableApplication {
    private static final Logger LOGGER = LoggerFactory.getLogger(AggregateTTLKTableApplication.class);
    public static void main(String[] args) {
        if(args.length != 2)
            throw new IllegalArgumentException("""
                    This program takes exactly 2 arguments:
                     - path to streams properties file
                     - path to admin properties file""");
        LOGGER.info("Starting " + AggregateTTLKTableApplication.class.getSimpleName());

        Properties streamsProps = loadProperties(args[0]);
        Properties adminProps = loadProperties(args[1]);

        final Duration MAX_AGE = Duration.of(Integer.parseInt(streamsProps.getProperty("table.topic.ttl.minutes")), ChronoUnit.MINUTES);
        final Duration SCAN_FREQUENCY = Duration.of(Integer.parseInt(streamsProps.getProperty("table.topic.ttl.scan.seconds")), ChronoUnit.SECONDS);
        final String PURGE_STATE_STORE_NAME = streamsProps.getProperty("table.topic.ttl.store.name");

        createTopics(adminProps);
        AggregateTTLKTableApplication app = new AggregateTTLKTableApplication(MAX_AGE, SCAN_FREQUENCY, PURGE_STATE_STORE_NAME);

        Topology topology = app.buildTopology(streamsProps);
        KafkaStreams kafkaStreams = new KafkaStreams(topology, streamsProps);

        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
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
            e.printStackTrace();
            LOGGER.error("An error occurred during streaming", e);
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
        try(AdminClient client = AdminClient.create(props)) {
            LOGGER.info("Creating topics for " + AggregateTTLKTableApplication.class.getSimpleName());
            List<NewTopic> topics = new ArrayList<>();

            topics.add(new NewTopic(
                    props.getProperty(props.getProperty("input.topic.name")),
                    Integer.parseInt(props.getProperty("input.topic.partitions")),
                    Short.parseShort(props.getProperty("input.topic.replication.factor"))));

            topics.add(new NewTopic(
                    props.getProperty(props.getProperty("table.topic.name")),
                    Integer.parseInt(props.getProperty("table.topic.partitions")),
                    Short.parseShort(props.getProperty("table.topic.replication.factor"))));

            topics.add(new NewTopic(
                    props.getProperty(props.getProperty("output.topic.name")),
                    Integer.parseInt(props.getProperty("output.topic.partitions")),
                    Short.parseShort(props.getProperty("output.topic.replication.factor"))));

            client.createTopics(topics);
            LOGGER.info("Created topics for " + AggregateTTLKTableApplication.class.getSimpleName());
        }
    }

    private final String purgeStateStoreName;
    private final Duration maxAge;
    private final Duration scanFrequency;

    public AggregateTTLKTableApplication(Duration maxAge, Duration scanFrequency, String purgeStateStoreName) {
        this.maxAge = maxAge;
        this.scanFrequency = scanFrequency;
        this.purgeStateStoreName = purgeStateStoreName;
    }

    public Topology buildTopology(Properties streamsProps) {
        final String inputTopicForStream = streamsProps.getProperty("input.topic.name");
        final String inputTopicForTable = streamsProps.getProperty("input.topic.name");
        final String outputTopic = streamsProps.getProperty("output.topic.name");
        StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> stream =
                builder.stream(inputTopicForStream, Consumed.with(Serdes.String(), Serdes.String()));

        final KStream<String, String> stream2 =
                builder.stream(inputTopicForTable, Consumed.with(Serdes.String(), Serdes.String()));

        final StoreBuilder<KeyValueStore<String, Long>> storeBuilder =
                Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(
                        purgeStateStoreName), Serdes.String(), Serdes.Long());

        builder.addStateStore(storeBuilder);

        KTable<String, AggregateObject> table = stream2
                .transform(() -> new TTLKTableTombstoneEmitter<String, String, KeyValue<String, ValueWrapper>>(
                        maxAge, scanFrequency, purgeStateStoreName), purgeStateStoreName)
                .groupByKey(Grouped.with(Serdes.String(), new JSONSerde<ValueWrapper>()))
                .aggregate(AggregateObject::new, (k, v, aggregate) -> {
                    System.out.println("aggregate() - value=" + v);
                    if(v.isDeleted()) return null;
                    return aggregate.add((String) v.getValue());
                }, Materialized.<String, AggregateObject, KeyValueStore<Bytes, byte[]>> as("event-store")
                        .withKeySerde(Serdes.String()).withValueSerde(new JSONSerde<AggregateObject>()));

        final KStream<String, String> joined = stream.leftJoin(table, (left, right) -> {
            if(right != null) {
                int size = right.getValues().size();
                return left + " " + right.getValues().get(size -1);
            }
            return left;
        });
        joined.to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }

    public void shutDown(KafkaStreams kafkaStreams, CountDownLatch latch) {
        LOGGER.info("Shutting down " + AggregateTTLKTableApplication.class.getSimpleName());
        kafkaStreams.close(Duration.of(5, ChronoUnit.SECONDS));
        latch.countDown();
        LOGGER.info("Shut down " + AggregateTTLKTableApplication.class.getSimpleName());
    }
}