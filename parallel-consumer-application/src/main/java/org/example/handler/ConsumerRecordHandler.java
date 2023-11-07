package org.example.handler;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumerRecordHandler<K,V> {
    public void handle(ConsumerRecord<K,V> records);
    public Number getProcessedRecords();
}
