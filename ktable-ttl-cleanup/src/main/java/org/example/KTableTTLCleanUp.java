package org.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.example.transformer.TTLEmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class KTableTTLCleanUp {
    private static final Logger LOGGER = LoggerFactory.getLogger(KTableTTLCleanUp.class);

    public static void main(String[] args) {

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

    private final Duration MAX_AGE;
    private final Duration SCAN_FREQUENCY;
    private final String STATE_STORE_NAME;
    public KTableTTLCleanUp(Duration maxAge, Duration scanFrequency, String stateStoreName) {
        this.MAX_AGE = maxAge;
        this.SCAN_FREQUENCY = scanFrequency;
        this.STATE_STORE_NAME = stateStoreName;
    }

    public Topology buildTopology(Properties streamsProps) {

        StreamsBuilder builder = new StreamsBuilder();

        String inputTopicForStream = streamsProps.getProperty("input.topic.name");
        String inputTopicForTable = streamsProps.getProperty("input.topic.table.name");
        String outputTopic = streamsProps.getProperty("output.topic.name");

        final KStream<String, String> stream = builder
                .stream(inputTopicForStream, Consumed.with(Serdes.String(), Serdes.String()));

        final KTable<String, String> table = stream
                .toTable(Named.as("table-store"),
                        Materialized.with(Serdes.String(), Serdes.String()));


        final KStream<String, String> joined = stream.leftJoin(table, (left, right) -> {
            System.out.println("JOINING left=" + left + " right=" + right);
            if(right != null)
                return left + " " + right;
            return left;
        });

        joined.to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        final StoreBuilder<KeyValueStore<String, Long>> storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(STATE_STORE_NAME),
                Serdes.String(),
                Serdes.Long());

        builder.addStateStore(storeBuilder);

        table.toStream()  //we just have to do this part for doing in the same topology but in another app, you can do as above
                .transform(() -> new TTLEmitter<String, String, KeyValue<String, String>>(MAX_AGE,
                        SCAN_FREQUENCY, STATE_STORE_NAME), STATE_STORE_NAME)
                .to(inputTopicForTable, Produced.with(Serdes.String(), Serdes.String()));

        System.out.println(builder);
        return builder.build();
    }


    public void shutDown(KafkaStreams kafkaStreams, CountDownLatch latch) {
        LOGGER.info("Shutting down " + KTableTTLCleanUp.class.getSimpleName());
        kafkaStreams.close(Duration.of(5, ChronoUnit.SECONDS));
        latch.countDown();
        LOGGER.info("Shut down " + KTableTTLCleanUp.class.getSimpleName());
    }
}