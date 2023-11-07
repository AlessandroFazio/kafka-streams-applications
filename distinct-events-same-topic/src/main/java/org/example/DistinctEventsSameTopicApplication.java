package org.example;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.example.avro.ClickEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.relation.RoleInfoNotFoundException;
import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class DistinctEventsSameTopicApplication {

    public static void main(String[] args) {
        if(args.length != 2)
            throw new IllegalArgumentException("""
                    This program takes exactly 2 arguments:
                     - a path to the streams config file
                     - a path to the admin config file""");

        Properties streamsProps = loadProperties(args[0]);
        Properties adminProps = loadProperties(args[1]);
        createTopics(adminProps);

        SpecificAvroSerde<ClickEvent> clickEventSerde = buildClickEventSerde(streamsProps);
        DistinctEventsSameTopicApplication app = new DistinctEventsSameTopicApplication();
        KafkaStreams kafkaStreams =
                new KafkaStreams(app.buildTopology(streamsProps, clickEventSerde), streamsProps);

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
        } finally {
            kafkaStreams.close();
        }
        System.exit(0);
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(DistinctEventsSameTopicApplication.class);
    private static final String storeName = "eventId-store";

    private static SpecificAvroSerde<ClickEvent> buildClickEventSerde(final Properties props) {
        SpecificAvroSerde<ClickEvent> clickEventSerde = new SpecificAvroSerde<>();
        clickEventSerde.configure((Map) props, false);
        return clickEventSerde;
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
            LOGGER.info("Creating topics for " + DistinctEventsSameTopicApplication.class.getSimpleName());
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
            LOGGER.info("Created topics for " + DistinctEventsSameTopicApplication.class.getSimpleName());
        }
    }

    private static class DuplicationTransformer<K, V, E> implements ValueTransformerWithKey<K, V, V > {
        private ProcessorContext context;
        private WindowStore<E, Long> eventIdStore;
        private final long leftDurationMs;
        private final long rightDurationMs;
        private final KeyValueMapper<K, V, E> idExtractor;

        public DuplicationTransformer(long maintainDurationPerEventMs, KeyValueMapper<K, V, E> idExtractor) {
            rightDurationMs = maintainDurationPerEventMs / 2;
            leftDurationMs = maintainDurationPerEventMs - rightDurationMs;
            this.idExtractor = idExtractor;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void init(ProcessorContext context) {
            this.context = context;
            eventIdStore = context.getStateStore(storeName);
        }

        @Override
        public V transform(K k, V v) {
            E eventId = idExtractor.apply(k,v);
            if(eventId == null) return v;

            V output = null;
            if(isDuplicate(eventId)) {
                updateTimestampOfExistingEventToPreventExpiry(eventId, context.timestamp());
            } else {
                output = v;
                rememberNewEvent(eventId, context.timestamp());
            }
            return output;
        }

        private boolean isDuplicate(E eventId) {
            final long eventTimestamp = context.timestamp();
            WindowStoreIterator<Long> iter = eventIdStore.fetch(
              eventId,
            eventTimestamp - leftDurationMs,
            eventTimestamp + rightDurationMs
            );
            boolean isDuplicate = iter.hasNext();
            iter.close();
            return isDuplicate;
        }

        private void updateTimestampOfExistingEventToPreventExpiry(E eventId, long eventTimestamp) {
            eventIdStore.put(eventId, eventTimestamp, eventTimestamp);
        }

        private void rememberNewEvent(E eventId, long eventTimestamp) {
            eventIdStore.put(eventId, eventTimestamp, eventTimestamp);
        }

        @Override
        public void close() {
        }
    }

    public Topology buildTopology(
            Properties streamsProps,
            final SpecificAvroSerde<ClickEvent> clickEventSerde
    ) {
        StreamsBuilder builder = new StreamsBuilder();
        String inputTopic = streamsProps.getProperty("input.topic.name");
        String outputTopic = streamsProps.getProperty("output.topic.name");

        Duration windowSize = Duration.ofMinutes(2);
        Duration retentionPeriod = windowSize;

        StoreBuilder<WindowStore<String, Long>> dedupStoreBuilder = Stores.windowStoreBuilder(
                Stores.persistentWindowStore(
                        storeName,
                        retentionPeriod,
                        windowSize,
                        false),
                Serdes.String(),
                Serdes.Long()
        );

        builder.addStateStore(dedupStoreBuilder);
        builder.stream(inputTopic, Consumed.with(Serdes.String(), clickEventSerde))
                .transformValues(() -> new DuplicationTransformer<>(windowSize.toMillis(), (k,v) -> v.getIp().toString()), storeName)
                .filter((k,v) -> v != null)
                .to(outputTopic, Produced.with(Serdes.String(), clickEventSerde));

        return builder.build();
    }

    public void shutDown(KafkaStreams kafkaStreams, CountDownLatch latch) {
        LOGGER.info("Shutting down " + DistinctEventsSameTopicApplication.class.getSimpleName());
        kafkaStreams.close(Duration.of(5, ChronoUnit.SECONDS));
        latch.countDown();
        LOGGER.info("Shut down " + DistinctEventsSameTopicApplication.class.getSimpleName());
    }
}