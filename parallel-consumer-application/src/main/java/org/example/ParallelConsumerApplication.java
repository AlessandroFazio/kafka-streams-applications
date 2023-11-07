package org.example;

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelStreamProcessor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.example.handler.ConsumerRecordHandler;
import org.example.handler.FileWritingConsumerRecordHandler;
import org.example.utils.PropertiesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.Random;
import java.util.random.RandomGenerator;

import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_SYNC;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;
import static io.confluent.parallelconsumer.ParallelStreamProcessor.createEosStreamProcessor;
import static java.util.Collections.singletonList;

public class ParallelConsumerApplication {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParallelConsumerApplication.class);

    public static void main(String[] args) throws IOException {
        if(args.length != 2)
            throw new IllegalArgumentException("""
                    This program takes exactly 2 arguements:
                     - path to the consumer.properties file
                     - path to the output file
                    """);

        final Random random = new Random();
        Properties consumerProps = PropertiesUtil.loadProperties(args[0]);
        String groupId = "parallel-consumer-app-" + random.nextInt();
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        String outputFile = args[1];

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        final ParallelConsumerOptions options = ParallelConsumerOptions.<String, String> builder()
                .ordering(KEY)
                .maxConcurrency(16)
                .consumer(consumer)
                .commitMode(PERIODIC_CONSUMER_SYNC)
                .build();
        ParallelStreamProcessor<String, String> eosStreamProcessor = createEosStreamProcessor(options);

        ConsumerRecordHandler<String, String> recordHandler =
                new FileWritingConsumerRecordHandler(Paths.get(outputFile));

        final ParallelConsumerApplication app =
                new ParallelConsumerApplication(eosStreamProcessor, recordHandler);

        Runtime.getRuntime().addShutdownHook(new Thread(app::shutDown));
        getShutDownThread(app).start();
        app.runConsume(consumerProps.getProperty("input.topic.name"));
    }

    private static Thread getShutDownThread(ParallelConsumerApplication app) {
        return new Thread(() -> {
            try {
                Thread.sleep(30000);
                app.shutDown();
            } catch (InterruptedException e) {
                throw new RuntimeException("Thread has been interrupted" + e);
            }
        });
    }

    private final ParallelStreamProcessor<String, String> parallelConsumer;
    private final ConsumerRecordHandler<String, String> recordsHandler;
    public ParallelConsumerApplication(ParallelStreamProcessor<String, String> parallelConsumer, ConsumerRecordHandler<String, String> recordsHandler) {
        this.parallelConsumer = parallelConsumer;
        this.recordsHandler = recordsHandler;
    }
    public void shutDown() {
        LOGGER.info("Shutting down " + ParallelConsumerApplication.class.getSimpleName());
        if(parallelConsumer != null)
            parallelConsumer.close();
    }

    public void runConsume(String topic) {

        LOGGER.info("ParallelConsumer is subscribing to topic {}", topic);

        parallelConsumer.subscribe(singletonList(topic));

        LOGGER.info("ParallelConsumer is starting to consume from topic {}", topic);

        parallelConsumer.poll(context ->
                recordsHandler.handle(context.getSingleConsumerRecord()));
    }
}