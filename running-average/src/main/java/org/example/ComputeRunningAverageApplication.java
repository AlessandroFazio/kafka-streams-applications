package org.example;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.checkerframework.checker.units.qual.C;
import org.example.demo.CountAndSum;
import org.example.demo.Rating;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CountDownLatch;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static java.util.Optional.ofNullable;
import static org.apache.kafka.streams.kstream.Grouped.with;

public class ComputeRunningAverageApplication {
    private static final Logger LOGGER = LoggerFactory.getLogger(ComputeRunningAverageApplication.class);
    public static void main(String[] args) {
        if(args.length != 2)
            throw new IllegalArgumentException("""
                    This program takes exactly 2 arguments:
                     - a path to the streamProps file
                     - a path to the adminProps file
                    """);
        LOGGER.info("Starting " + ComputeRunningAverageApplication.class.getSimpleName());

        Properties streamProps = loadProperties(args[0]);

        Properties adminProps = loadProperties(args[1]);

        ComputeRunningAverageApplication app = new ComputeRunningAverageApplication();
        createTopics(adminProps);

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
            e.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
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

    public static void createTopics(Properties adminProps) {
        try(AdminClient client = AdminClient.create(adminProps)) {
            List<NewTopic> topics = new ArrayList<>();

            topics.add(new NewTopic(
                    adminProps.getProperty("input.ratings.topic.name"),
                    Integer.parseInt(adminProps.getProperty("input.ratings.topic.partitions")),
                    Short.parseShort(adminProps.getProperty("input.ratings.topic.replication.factor"))));

            topics.add(new NewTopic(
                    adminProps.getProperty("output.rating-averages.topic.name"),
                    Integer.parseInt(adminProps.getProperty("output.rating-averages.topic.partitions")),
                    Short.parseShort(adminProps.getProperty("output.rating-averages.topic.replication.factor"))));

            client.createTopics(topics);
            topics.forEach(topic -> LOGGER.info("Created topic {}", topic));
        }
    }

    public static SpecificAvroSerde<CountAndSum> getCountAndSumSerde(Properties envProps) {
        SpecificAvroSerde<CountAndSum> serde = new SpecificAvroSerde<>();
        serde.configure(getSerdeConfig(envProps), false);
        return serde;
    }

    public static SpecificAvroSerde<Rating> getRatingSerde(Properties envProps) {
        SpecificAvroSerde<Rating> serde = new SpecificAvroSerde<>();
        serde.configure(getSerdeConfig(envProps), false);
        return serde;
    }

    protected static Map<String, String> getSerdeConfig(Properties envProps) {
        final HashMap<String, String> map = new HashMap<>();

        final String srUrlConfig = envProps.getProperty(SCHEMA_REGISTRY_URL_CONFIG);
        map.put(SCHEMA_REGISTRY_URL_CONFIG, ofNullable(srUrlConfig).orElse(""));
        return map;
    }

    protected static KTable<Long, Double> getRatingAverageTable(KStream<Long, Rating> ratings,
                                                                String avgRatingsTopicName,
                                                                SpecificAvroSerde<CountAndSum> countAndSumSerde) {

        // Grouping Ratings
        KGroupedStream<Long, Double> ratingsById = ratings
                .map((key, rating) -> new KeyValue<>(rating.getMovieId(), rating.getRating()))
                .groupByKey(Grouped.with(Serdes.Long(), Serdes.Double()));

        final KTable<Long, CountAndSum> ratingCountAndSum =
                ratingsById.aggregate(() -> new CountAndSum(0L, 0.0),
                        (key, value, aggregate) -> {
                            aggregate.setCount(aggregate.getCount() + 1);
                            aggregate.setSum(aggregate.getSum() + value);
                            return aggregate;
                        },
                        Materialized.with(Serdes.Long(), countAndSumSerde));

        final KTable<Long, Double> ratingAverage =
                ratingCountAndSum.mapValues(value -> value.getSum() / value.getCount(),
                        Materialized.as("average-ratings"));

        // persist the result in topic
        ratingAverage.toStream()
                .to(avgRatingsTopicName, Produced.with(Serdes.Long(), Serdes.Double()));
        return ratingAverage;
    }

    //region buildTopology
    private Topology buildTopology(Properties envProps) {

        final String ratingTopicName = envProps.getProperty("input.ratings.topic.name");
        final String avgRatingsTopicName = envProps.getProperty("output.rating-averages.topic.name");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<Long, Rating> ratingStream = builder.stream(ratingTopicName,
                Consumed.with(Serdes.Long(), getRatingSerde(envProps)));

        getRatingAverageTable(ratingStream, avgRatingsTopicName, getCountAndSumSerde(envProps));

        // finish the topology
        return builder.build();
    }
    public void shutDown(KafkaStreams kafkaStreams, CountDownLatch latch) {
        LOGGER.info("Shutting down the " + ComputeRunningAverageApplication.class.getSimpleName());
        kafkaStreams.close(Duration.of(5, ChronoUnit.SECONDS));
        latch.countDown();
    }
}