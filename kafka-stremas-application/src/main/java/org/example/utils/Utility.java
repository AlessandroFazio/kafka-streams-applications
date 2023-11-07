package org.example.utils;

import com.github.javafaker.Faker;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class Utility implements AutoCloseable {
    private final Logger logger = LoggerFactory.getLogger(Utility.class);
    private ExecutorService executorService = Executors.newFixedThreadPool(1);

    public class Randomizer implements AutoCloseable, Runnable {
        private Properties producerProps;
        private Producer<String, String> producer;
        private String topic;
        private boolean closed = false;

        public Randomizer(Properties producerProps, String topic) {
            this.topic = topic;
            this.producerProps = producerProps;
            this.producerProps.setProperty("client.id", "faker");

        }
        @Override
        public void close() throws Exception {
            closed = true;
        }

        @Override
        public void run() {
            try(KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
                Faker faker = new Faker();
                while(!closed) {
                    try {
                        Object result = producer.send(
                                new ProducerRecord<>(
                                topic, faker.chuckNorris().fact())).get();
                        Thread.sleep(5000);
                    } catch (InterruptedException | ExecutionException e) {
                    }
                }
            } catch (Exception e) {
                logger.error(e.toString());
            }
        }
    }

    public Randomizer newRandomizer(Properties properties, String topic) {
        Randomizer rnd = new Randomizer(properties, topic);
        executorService.submit(rnd);
        return rnd;
    }

    public void createTopics(final Properties allProps, List<NewTopic> newTopics)
            throws ExecutionException, InterruptedException, TimeoutException {
        try(AdminClient adminClient = AdminClient.create(allProps)) {
            adminClient.createTopics(newTopics).values().forEach((topic, future) -> {
                try {
                    future.get();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            logger.info("Asking cluster for topic descriptions");
            Collection<String> topicNames = newTopics.stream()
                    .map(NewTopic::name)
                    .collect(Collectors.toCollection(LinkedList::new));

            adminClient
                    .describeTopics(topicNames)
                    .allTopicNames()
                    .get(10, TimeUnit.SECONDS)
                    .forEach((name, desc) ->
                        logger.info("Topic Description: {}", desc.toString()));
        }
    }

    @Override
    public void close() {
        if(executorService != null) {
            executorService.shutdown();
            executorService = null;
        }
    }
}
