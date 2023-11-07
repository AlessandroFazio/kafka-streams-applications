package org.example.joiner;

import org.example.avro.Album;
import org.example.avro.MusicInterest;
import org.example.avro.TrackPurchase;
import org.junit.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MusicInterestJoinerTest {
    @Test
    public void apply() {
        Album album = Album.newBuilder()
                .setId(445L)
                .setArtist("Gue Pequeno")
                .setTitle("Soldi Everywhere")
                .setGenre("Rap")
                .build();

        TrackPurchase trackPurchase = TrackPurchase.newBuilder()
                .setId(666L)
                .setPrice(15.00)
                .setSongTitle("MyMoney")
                .setAlbumId(455L)
                .build();

        MusicInterest expected = MusicInterest.newBuilder()
                .setId("445-666")
                .setGenre("Rap")
                .setArtist("Gue Pequeno")
                .build();

        MusicInterestJoiner joiner = new MusicInterestJoiner();
        MusicInterest actual = joiner.apply(trackPurchase, album);

        assertEquals(expected, actual);
    }
}
