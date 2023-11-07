package org.example.joiner;

import org.apache.kafka.streams.kstream.ValueJoiner;
import org.example.avro.Album;
import org.example.avro.MusicInterest;
import org.example.avro.TrackPurchase;

public class MusicInterestJoiner implements ValueJoiner<TrackPurchase, Album, MusicInterest> {
    @Override
    public MusicInterest apply(TrackPurchase trackPurchase, Album album) {
        return MusicInterest.newBuilder()
                .setId(album.getId() + "-" + trackPurchase.getId())
                .setArtist(album.getArtist())
                .setGenre(album.getGenre())
                .build();
    }
}
