package org.example;

import com.fasterxml.jackson.databind.annotation.JsonAppend;
import com.github.javafaker.Faker;
import org.apache.hadoop.fs.impl.StoreImplementationUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.example.producer.MovieProducer;
import org.example.producer.RatingProducer;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class DataProducer {
    static int size = 20;
    static long[] uuidPool = new long[size];
    static String[] titles = new String[size];
    static int[] years = new int[size];
    static private Random RANDOM = new Random();
    static private Faker FAKER = new Faker();

    static {
        for (int i = 0; i < size; i++) uuidPool[i] = RANDOM.nextLong(10L);
        for (int i = 0; i < size; i++) titles[i] = FAKER.book().title();
        for (int i = 0; i < size; i++) years[i] = RANDOM.nextInt(1990, 2023);
    }

    public static void main(String[] args) {

    }

    private String name;
    private RatingProducer ratingProducer;
    private MovieProducer movieProducer;

    public DataProducer(RatingProducer ratingProducer, MovieProducer movieProducer, String name) {
        this.ratingProducer = ratingProducer;
        this.movieProducer = movieProducer;
        this.name = name;
    }

    public void run() {
        ratingProducer.produceRecords(uuidPool);
        movieProducer.produceRecords(uuidPool, titles, years);
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
}
