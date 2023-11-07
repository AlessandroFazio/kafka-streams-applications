package org.example.handler;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.concurrent.atomic.AtomicInteger;

public abstract class ConsumerRecordHandlerImpl<K,V> implements ConsumerRecordHandler<K,V> {
    private AtomicInteger totalProcessedRecords;

    @Override
    public void handle(ConsumerRecord<K, V> record) {
        process(record);
        totalProcessedRecords.incrementAndGet();
    }

    public abstract void process(ConsumerRecord<K,V> record);

    @Override
    public Integer getProcessedRecords() {
        return totalProcessedRecords.get();
    }
}
