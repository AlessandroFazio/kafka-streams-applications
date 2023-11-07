package org.example;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.example.avro.Movie;
import org.example.avro.RatedMovie;
import org.example.avro.Rating;
import org.example.joiner.MovieRatingJoiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class JoinStreamToTableApplication {
    private static final Logger LOGGER = LoggerFactory.getLogger(JoinStreamToTableApplication.class);
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
        JoinStreamToTableApplication app = new JoinStreamToTableApplication();
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
            LOGGER.info("Creating topics for " + JoinStreamToTableApplication.class.getSimpleName());
            List<NewTopic> topics = new ArrayList<>();

            topics.add(new NewTopic(
                    props.getProperty(props.getProperty("movie.topic.name")),
                    Integer.parseInt(props.getProperty("movie.topic.partitions")),
                    Short.parseShort(props.getProperty("movie.topic.replication.factor"))));

            topics.add(new NewTopic(
                    props.getProperty(props.getProperty("rekeyed.movie.topic.name")),
                    Integer.parseInt(props.getProperty("rekeyed.movie.topic.partitions")),
                    Short.parseShort(props.getProperty("rekeyed.movie.topic.replication.factor"))));

            topics.add(new NewTopic(
                    props.getProperty(props.getProperty("rating.topic.name")),
                    Integer.parseInt(props.getProperty("rating.topic.partitions")),
                    Short.parseShort(props.getProperty("rating.topic.replication.factor"))));

            topics.add(new NewTopic(
                    props.getProperty(props.getProperty("rated-movie.topic.name")),
                    Integer.parseInt(props.getProperty("rated-movie.topic.partitions")),
                    Short.parseShort(props.getProperty("rated-movie.topic.replication.factor"))));

            client.createTopics(topics);
            LOGGER.info("Created topics for " + JoinStreamToTableApplication.class.getSimpleName());
        }
    }

    public Topology buildTopology(Properties streamPros) {
        StreamsBuilder builder = new StreamsBuilder();
        String movieInputTopic = streamPros.getProperty("movie.topic.name");
        String rekeyedMovieTopic = streamPros.getProperty("rekeyed.movie.topic.name");
        String ratingInputTopic = streamPros.getProperty("rating.topic.name");
        String ratedMovieTopic = streamPros.getProperty("rated.movies.topic.name");

        SpecificAvroSerde<Movie> movieSerde = getSpecificAvroSerde(streamPros, false);
        SpecificAvroSerde<Rating> ratingSerde = getSpecificAvroSerde(streamPros, false);
        SpecificAvroSerde<RatedMovie> ratedMovieSerde = getSpecificAvroSerde(streamPros, false);

        MovieRatingJoiner joiner = new MovieRatingJoiner();

        KStream<String, Movie> movieStream = builder
                .stream(movieInputTopic, Consumed.with(Serdes.String(), movieSerde))
                .map((key, movie) -> new KeyValue<>(String.valueOf(movie.getId()), movie));

        movieStream.to(rekeyedMovieTopic, Produced.with(Serdes.String(), movieSerde));

        KTable<String, Movie> movieTable = builder
                .table(rekeyedMovieTopic, Consumed.with(Serdes.String(), movieSerde));

        KStream<String, Rating> ratingStream = builder
                .stream(ratingInputTopic, Consumed.with(Serdes.String(), ratingSerde))
                .map((key, movie) -> new KeyValue<>(String.valueOf(movie.getId()), movie));

        KStream<String, RatedMovie> ratedMovieStream = ratingStream.join(movieTable, joiner);

        ratedMovieStream.to(ratedMovieTopic, Produced.with(Serdes.String(), ratedMovieSerde));

        return builder.build();
    }

    private <T extends SpecificRecord> SpecificAvroSerde<T> getSpecificAvroSerde (Properties props, boolean isKey) {
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
        LOGGER.info("Shutting down " + JoinStreamToTableApplication.class.getSimpleName());
        kafkaStreams.close(Duration.of(5, ChronoUnit.SECONDS));
        latch.countDown();
        LOGGER.info("Shut down " + JoinStreamToTableApplication.class.getSimpleName());
    }
}