package org.example;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.*;
import org.example.avro.Movie;
import org.example.avro.RatedMovie;
import org.example.avro.Rating;
import org.junit.After;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class JoinStreamToTableApplicationTest {
    private static final String TEST_CONFIG_FILE = "src/test/resources/streams.properties";
    private TopologyTestDriver testDriver;

    private <T extends SpecificRecord> SpecificAvroSerializer<T> getSpecificAvroSerializer (Properties props, boolean isKey) {
        SpecificAvroSerializer<T> specificAvroSerializer = new SpecificAvroSerializer<>();
        specificAvroSerializer.configure(getSerdeConfig(props), isKey);
        return specificAvroSerializer;
    }

    private <T extends SpecificRecord> SpecificAvroDeserializer<T> getSpecificAvroDeserializer (Properties props, boolean isKey) {
        SpecificAvroDeserializer<T> specificAvroDeserializer = new SpecificAvroDeserializer<>();
        specificAvroDeserializer.configure(getSerdeConfig(props), isKey);
        return specificAvroDeserializer;
    }

    private Map<String, String> getSerdeConfig(Properties props) {
        Map<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put("schema.registry.url", props.getProperty("schema.registry.url"));
        return serdeConfig;
    }

    private List<RatedMovie> readOutputTopic(TopologyTestDriver testDriver,
                                             String topic,
                                             SpecificAvroDeserializer<RatedMovie> ratedMovieDeserializer
    ) {
        List<RatedMovie> ratedMovies = new ArrayList<>();
        final TestOutputTopic<String, RatedMovie> testOutputTopic =
                testDriver.createOutputTopic(topic, Serdes.String().deserializer(), ratedMovieDeserializer);

        testOutputTopic
                .readKeyValuesToList()
                .forEach(record -> {
                    if(record != null) ratedMovies.add(record.value);
                });
        return ratedMovies;
    }

    @Test
    public void testJoin() throws IOException {
        JoinStreamToTableApplication app = new JoinStreamToTableApplication();
        Properties streamProps = loadProperties(TEST_CONFIG_FILE);

        String tableInputTopic = streamProps.getProperty("movie.topic.name");
        String streamTopic = streamProps.getProperty("rating.topic.name");
        String outputTopic = streamProps.getProperty("rated.movies.topic.name");

        streamProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

        Topology topology = app.buildTopology(streamProps);
        testDriver = new TopologyTestDriver(topology, streamProps);

        Serializer<String> stringSerializer = Serdes.String().serializer();
        Serializer<Movie> movieSerializer = getSpecificAvroSerializer(streamProps, false);
        Serializer<Rating> ratingSerializer = getSpecificAvroSerializer(streamProps, false);
        SpecificAvroDeserializer<RatedMovie> ratedMovieDeserializer = getSpecificAvroDeserializer(streamProps, false);

        List<Movie> movies = new ArrayList<>();
        movies.add(Movie.newBuilder().setId(294).setTitle("Die Hard").setReleaseYear(1988).build());
        movies.add(Movie.newBuilder().setId(354).setTitle("Tree of Life").setReleaseYear(2011).build());
        movies.add(Movie.newBuilder().setId(782).setTitle("A Walk in the Clouds").setReleaseYear(1998).build());
        movies.add(Movie.newBuilder().setId(128).setTitle("The Big Lebowski").setReleaseYear(1998).build());
        movies.add(Movie.newBuilder().setId(780).setTitle("Super Mario Bros.").setReleaseYear(1993).build());

        List<Rating> ratings = new ArrayList<>();
        ratings.add(Rating.newBuilder().setId(294).setRating(8.2).build());
        ratings.add(Rating.newBuilder().setId(294).setRating(8.5).build());
        ratings.add(Rating.newBuilder().setId(354).setRating(9.9).build());
        ratings.add(Rating.newBuilder().setId(354).setRating(9.7).build());
        ratings.add(Rating.newBuilder().setId(782).setRating(7.8).build());
        ratings.add(Rating.newBuilder().setId(782).setRating(7.7).build());
        ratings.add(Rating.newBuilder().setId(128).setRating(8.7).build());
        ratings.add(Rating.newBuilder().setId(128).setRating(8.4).build());
        ratings.add(Rating.newBuilder().setId(780).setRating(2.1).build());

        List<RatedMovie> ratedMovies = new ArrayList<>();
        ratedMovies.add(RatedMovie.newBuilder().setTitle("Die Hard").setId(294).setReleaseYear(1988).setRating(8.2).build());
        ratedMovies.add(RatedMovie.newBuilder().setTitle("Die Hard").setId(294).setReleaseYear(1988).setRating(8.5).build());
        ratedMovies.add(RatedMovie.newBuilder().setTitle("Tree of Life").setId(354).setReleaseYear(2011).setRating(9.9).build());
        ratedMovies.add(RatedMovie.newBuilder().setTitle("Tree of Life").setId(354).setReleaseYear(2011).setRating(9.7).build());
        ratedMovies.add(RatedMovie.newBuilder().setId(782).setTitle("A Walk in the Clouds").setReleaseYear(1998).setRating(7.8).build());
        ratedMovies.add(RatedMovie.newBuilder().setId(782).setTitle("A Walk in the Clouds").setReleaseYear(1998).setRating(7.7).build());
        ratedMovies.add(RatedMovie.newBuilder().setId(128).setTitle("The Big Lebowski").setReleaseYear(1998).setRating(8.7).build());
        ratedMovies.add(RatedMovie.newBuilder().setId(128).setTitle("The Big Lebowski").setReleaseYear(1998).setRating(8.4).build());
        ratedMovies.add(RatedMovie.newBuilder().setId(780).setTitle("Super Mario Bros.").setReleaseYear(1993).setRating(2.1).build());

        final TestInputTopic<String, Movie> movieTestInputTopic =
                testDriver.createInputTopic(tableInputTopic, stringSerializer, movieSerializer);

        for(Movie movie: movies) movieTestInputTopic.pipeInput(String.valueOf(movie.getId()), movie);

        final TestInputTopic<String, Rating> ratingTestInputTopic =
                testDriver.createInputTopic(streamTopic, stringSerializer, ratingSerializer);

        for(Rating rating: ratings) ratingTestInputTopic.pipeInput(String.valueOf(rating.getId()), rating);

        List<RatedMovie> actualOutput = readOutputTopic(testDriver, outputTopic, ratedMovieDeserializer);

        assertEquals(ratedMovies, actualOutput);
    }

    private Properties loadProperties(String fileName) {
        Properties props = new Properties();
        try(FileInputStream fileInputStream = new FileInputStream(fileName)) {
            props.load(fileInputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return props;
    }

    @After
    public void cleanup() {
        testDriver.close();
    }
}
