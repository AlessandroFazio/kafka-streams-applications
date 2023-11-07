package org.example.handler;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.file.StandardOpenOption.*;
import static java.util.Collections.singletonList;

public class FileWritingConsumerRecordHandler extends ConsumerRecordHandlerImpl<String, String> {
    private final Path path;
    public FileWritingConsumerRecordHandler(Path path) {
        this.path = path;
    }
    @Override
    public void process(ConsumerRecord<String, String> record) {
        try{
            Files.write(path, singletonList(record.value()), CREATE, APPEND, WRITE);
        } catch (IOException e) {
            throw new RuntimeException("Unable to write the record to file", e);
        }
    }
}
