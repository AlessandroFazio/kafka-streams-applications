package org.example.handler;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.streams.processor.api.Record;

import java.util.List;

public interface RecordsHandler<K,V> {
    public void handle(ConsumerRecords<K,V> records);
}
