package org.example;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.example.avro.LoginTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class ScheduleOperations {
    private static Logger LOGGER = LoggerFactory.getLogger(ScheduleOperations.class);
    public static void main(String[] args) {
        if(args.length != 2)
            throw new IllegalArgumentException("""
                    This program takes exactly 2 arguments:
                     - a path to the streams config file
                     - a path to the admin config file""");

        LOGGER.info("Starting " + ScheduleOperations.class);

        Properties streamsProps = loadProperties(args[0]);
        Properties adminProps = loadProperties(args[1]);
        createTopics(adminProps);

        ScheduleOperations app =
                new ScheduleOperations();
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
        try(AdminClient client = AdminClient.create(props)) {
            LOGGER.info("Creating topics for " + ScheduleOperations.class.getSimpleName());
            List<NewTopic> topics = new ArrayList<>();

            topics.add(new NewTopic(
                    props.getProperty(props.getProperty("input.topic.name")),
                    Integer.parseInt(props.getProperty("input.topic.partitions")),
                    Short.parseShort(props.getProperty("input.topic.replication.factor"))));

            topics.add(new NewTopic(
                    props.getProperty(props.getProperty("output.topic.name")),
                    Integer.parseInt(props.getProperty("output.topic.partitions")),
                    Short.parseShort(props.getProperty("output.topic.replication.factor"))));

            client.createTopics(topics);
            LOGGER.info("Created topics for " + ScheduleOperations.class.getSimpleName());
        }
    }

    public Topology buildTopology(Properties streamsProps) {
        StreamsBuilder builder = new StreamsBuilder();
        String inputTopic = streamsProps.getProperty("input.topic.name");
        String outputTopic = streamsProps.getProperty("output.topic.name");
        SpecificAvroSerde<LoginTime> loginTimeSerde = getSpecificAvroSerde(streamsProps);
        String loginTimeStore = "logintime-store";

        KStream<String, LoginTime> loginTimeKStream = builder
                .stream(inputTopic, Consumed.with(Serdes.String(), loginTimeSerde));

        loginTimeKStream
                .transform(getTransformerSupplier(loginTimeStore), Named.as("max-login-time-transformer"), loginTimeStore)
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        return builder.build();
    }

    private TransformerSupplier<String, LoginTime, KeyValue<String, Long>> getTransformerSupplier(String storeName) {
        return () -> new Transformer<String, LoginTime, KeyValue<String, Long>>() {
            private ProcessorContext context;
            private KeyValueStore<String, Long> store;
            @Override
            public void init(ProcessorContext context) {
                this.context = context;
                this.store = this.context.getStateStore(storeName);
                this.context.schedule(Duration.of(5, ChronoUnit.SECONDS), PunctuationType.STREAM_TIME, this::streamTimePunctuator);
                this.context.schedule(Duration.of(5, ChronoUnit.SECONDS), PunctuationType.WALL_CLOCK_TIME, this::wallClockTimePunctuator);
            }

            private void streamTimePunctuator(long timestamp) {
                try(KeyValueIterator<String, Long> iter = store.all()) {
                    while(iter.hasNext()) {
                        KeyValue<String, Long> keyValue = iter.next();
                        store.put(keyValue.key, 0L);
                    }
                }
                System.out.println("@" + new Date(timestamp) +" Reset all view-times to zero");
            }

            private void wallClockTimePunctuator(long timestamp) {
                Long maxValue = Long.MIN_VALUE;
                String maxValueKey = "";
                try(KeyValueIterator<String, Long> iter = store.all()) {
                    while(iter.hasNext()) {
                        KeyValue<String, Long> keyValue = iter.next();
                        if(keyValue.value > maxValue) {
                            maxValue = keyValue.value;
                            maxValueKey = keyValue.key;
                        }
                    }
                }
                context.forward(maxValueKey + " @" + new Date(timestamp), maxValue);
            }

            @Override
            public KeyValue<String, Long> transform(String key, LoginTime value) {
                Long currentVT = store.putIfAbsent(key, value.getLogintime());
                if(currentVT != null) {
                    store.put(key, currentVT + value.getLogintime());
                }
                return null;
            }

            @Override
            public void close() {
            }
        };
    }

    private <T extends SpecificRecord> SpecificAvroSerde<T> getSpecificAvroSerde(Properties streamsProps) {
        SpecificAvroSerde<T> specificAvroSerde = new SpecificAvroSerde<>();
        Map<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                streamsProps.getProperty("schema.registry.url"));
        specificAvroSerde.configure(serdeConfig, false);
        return specificAvroSerde;
    }

    public void shutDown(KafkaStreams kafkaStreams, CountDownLatch latch) {
        LOGGER.info("Shutting down " + ScheduleOperations.class.getSimpleName());
        kafkaStreams.close(Duration.of(5, ChronoUnit.SECONDS));
        latch.countDown();
        LOGGER.info("Shut down " + ScheduleOperations.class.getSimpleName());
    }
}