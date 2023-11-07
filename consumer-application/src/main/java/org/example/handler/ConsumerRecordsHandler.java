package org.example.handler;

import org.apache.kafka.clients.consumer.ConsumerRecords;

public class ConsumerRecordsHandler<K,V> implements RecordsHandler<K,V> {
    @Override
    public void handle(ConsumerRecords<K, V> records) {
        records.forEach(record ->
                System.out.println("Record: " + record.value()));
    }
}
