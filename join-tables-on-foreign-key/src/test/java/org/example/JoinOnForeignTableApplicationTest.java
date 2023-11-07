package org.example;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.*;
import org.example.avro.Album;
import org.example.avro.MusicInterest;
import org.example.avro.TrackPurchase;
import org.junit.After;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class JoinOnForeignTableApplicationTest {
    private static final String INPUT_CONFIG_FILE = "src/test/resources/test.properties";

    @Test
    public void testJoins() {
        JoinOnOForeignTableApplication appToTest = new JoinOnOForeignTableApplication();

        Properties testProps = loadProperties(INPUT_CONFIG_FILE);

        final String albumInputTopic = testProps.getProperty("album.topic.name");
        final String trackPurchaseInputTopic = testProps.getProperty("tracks.purchase.topic.name");
        final String musicInterestOutputTopic = testProps.getProperty("music.interest.topic.name");

        Topology topology = appToTest.buildTopology(testProps);

        try(final TopologyTestDriver testDriver = new TopologyTestDriver(topology, testProps)) {
             final SpecificAvroSerializer<Album> albumSerializer =
                     getSpecificAvroSerializer(testProps, false);
             final SpecificAvroSerializer<TrackPurchase> trackPurchaseSerializer =
                     getSpecificAvroSerializer(testProps, false);
             final SpecificAvroSerializer<MusicInterest> musicInterestSerializer =
                     getSpecificAvroSerializer(testProps, false);
             final Serializer<Long> longSerializer = Serdes.Long().serializer();
             final Deserializer<Long> longDeserializer = Serdes.Long().deserializer();

             final Deserializer<MusicInterest> musicInterestDeserializer =
                     getSpecificAvroDeserializer(testProps, false);

            TestInputTopic<Long, Album> albumTestInputTopic = testDriver
                    .createInputTopic(albumInputTopic, longSerializer, albumSerializer);

            TestInputTopic<Long, TrackPurchase> trackPurchaseTestInputTopic = testDriver
                    .createInputTopic(trackPurchaseInputTopic, longSerializer, trackPurchaseSerializer);

            TestOutputTopic<Long, MusicInterest> musicInterestTestOutputTopic = testDriver
                    .createOutputTopic(musicInterestOutputTopic, longDeserializer, musicInterestDeserializer);

            final List<Album> albums = new ArrayList<>();
            albums.add(Album.newBuilder().setId(5L).setTitle("Physical Graffiti").setArtist("Led Zeppelin").setGenre("Rock").build());
            albums.add(Album.newBuilder().setId(6L).setTitle("Highway to Hell").setArtist("AC/DC").setGenre("Rock").build());
            albums.add(Album.newBuilder().setId(7L).setTitle("Radio").setArtist("LL Cool J").setGenre("Hip hop").build());
            albums.add(Album.newBuilder().setId(8L).setTitle("King of Rock").setArtist("Run-D.M.C").setGenre("Rap rock").build());

            final List<TrackPurchase> trackPurchases = new ArrayList<>();
            trackPurchases.add(TrackPurchase.newBuilder().setId(100).setAlbumId(5L).setSongTitle("Houses Of The Holy").setPrice(0.99).build());
            trackPurchases.add(TrackPurchase.newBuilder().setId(101).setAlbumId(8L).setSongTitle("King Of Rock").setPrice(0.99).build());
            trackPurchases.add(TrackPurchase.newBuilder().setId(102).setAlbumId(6L).setSongTitle("Shot Down In Flames").setPrice(0.99).build());
            trackPurchases.add(TrackPurchase.newBuilder().setId(103).setAlbumId(7L).setSongTitle("Rock The Bells").setPrice(0.99).build());
            trackPurchases.add(TrackPurchase.newBuilder().setId(104).setAlbumId(8L).setSongTitle("Can You Rock It Like This").setPrice(0.99).build());
            trackPurchases.add(TrackPurchase.newBuilder().setId(105).setAlbumId(6L).setSongTitle("Highway To Hell").setPrice(0.99).build());
            trackPurchases.add(TrackPurchase.newBuilder().setId(106).setAlbumId(5L).setSongTitle("Kashmir").setPrice(0.99).build());

            final List<MusicInterest> expectedMusicInterests = new ArrayList<>();
            expectedMusicInterests.add(MusicInterest.newBuilder().setId("5-100").setGenre("Rock").setArtist("Led Zeppelin").build());
            expectedMusicInterests.add(MusicInterest.newBuilder().setId("8-101").setGenre("Rap rock").setArtist("Run-D.M.C").build());
            expectedMusicInterests.add(MusicInterest.newBuilder().setId("6-102").setGenre("Rock").setArtist("AC/DC").build());
            expectedMusicInterests.add(MusicInterest.newBuilder().setId("7-103").setGenre("Hip hop").setArtist("LL Cool J").build());
            expectedMusicInterests.add(MusicInterest.newBuilder().setId("8-104").setGenre("Rap rock").setArtist("Run-D.M.C").build());
            expectedMusicInterests.add(MusicInterest.newBuilder().setId("6-105").setGenre("Rock").setArtist("AC/DC").build());
            expectedMusicInterests.add(MusicInterest.newBuilder().setId("5-106").setGenre("Rock").setArtist("Led Zeppelin").build());

            for(Album album: albums) albumTestInputTopic.pipeInput(album.getId(), album);

            for(TrackPurchase purchase: trackPurchases) trackPurchaseTestInputTopic.pipeInput(purchase.getId(), purchase);

            final List<MusicInterest> actualMusicInterests = musicInterestTestOutputTopic.readValuesToList();

            assertEquals(expectedMusicInterests, actualMusicInterests);
        }
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

    private <T extends SpecificRecord> SpecificAvroSerializer<T> getSpecificAvroSerializer (
            Properties props, boolean isKey
    ) {
        SpecificAvroSerializer<T> specificAvroSerializer = new SpecificAvroSerializer<>();
        specificAvroSerializer.configure(getSerdeConfig(props), isKey);
        return specificAvroSerializer;
    }

    private <T extends SpecificRecord> SpecificAvroDeserializer<T> getSpecificAvroDeserializer (
            Properties props, boolean isKey
    ) {
        SpecificAvroDeserializer<T> specificAvroDeserializer = new SpecificAvroDeserializer<>();
        specificAvroDeserializer.configure(getSerdeConfig(props), isKey);
        return specificAvroDeserializer;
    }

    private Map<String, String> getSerdeConfig(Properties props) {
        Map<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put("schema.registry.url", props.getProperty("schema.registry.url"));
        return serdeConfig;
    }
}
