package org.example.joiner;

import org.apache.kafka.streams.kstream.ValueJoiner;
import org.example.avro.Movie;
import org.example.avro.RatedMovie;
import org.example.avro.Rating;

public class MovieRatingJoiner implements ValueJoiner<Rating, Movie, RatedMovie> {
    @Override
    public RatedMovie apply(Rating rating, Movie movie) {
        return RatedMovie.newBuilder()
                .setId(movie.getId())
                .setTitle(movie.getTitle())
                .setReleaseYear(movie.getReleaseYear())
                .setRating(rating.getRating())
                .build();
    }
}
