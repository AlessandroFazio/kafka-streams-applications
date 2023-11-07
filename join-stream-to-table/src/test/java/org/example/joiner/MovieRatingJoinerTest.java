package org.example.joiner;

import org.example.avro.Movie;
import org.example.avro.RatedMovie;
import org.example.avro.Rating;
import org.junit.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MovieRatingJoinerTest {
    @Test
    public void apply() {
        RatedMovie actual;

        Movie movie = Movie.newBuilder().setId(354).setTitle("Tree of Life").setReleaseYear(2001).build();
        Rating rating = Rating.newBuilder().setId(354).setRating(9.0).build();

        MovieRatingJoiner joiner = new MovieRatingJoiner();
        RatedMovie expected = RatedMovie.newBuilder()
                .setId(354)
                .setTitle("Tree of Life")
                .setReleaseYear(2001)
                .setRating(9.0)
                .build();

        actual = joiner.apply(rating, movie);

        assertEquals(actual, expected);
    }
}
