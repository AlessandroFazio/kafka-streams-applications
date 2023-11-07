package org.example.transformer;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;

public class TTLEmitter<K,V,R> implements Transformer<K,V,R> {
    private ProcessorContext context;
    private KeyValueStore<K,Long> stateStore;
    private String purgeStoreName;
    private Duration maxAge;
    private Duration scanFrequency;


    public TTLEmitter(
            final Duration scanFrequency, final Duration maxAge, final String storeName
    ) {
        this.scanFrequency = scanFrequency;
        this.maxAge = maxAge;
        this.purgeStoreName = storeName;
    }


    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.stateStore = this.context.getStateStore(purgeStoreName);
        context.schedule(scanFrequency, PunctuationType.STREAM_TIME, (timestamp) -> {

            final long cutoff = timestamp - maxAge.toMillis();
            KeyValueIterator<K, Long> iter = stateStore.all();
            while(iter.hasNext()) {
                KeyValue<K, Long> record = iter.next();
                if(record.value != null  && record.value < cutoff) {
                    System.out.println("Forwarding: NULL");
                    context.forward(record.key, null);
                }
            }
        });
    }

    @Override
    public R transform(K key, V value) {
        if(value == null) {
            System.out.printf("Cleaning Key: %s", key);
            stateStore.delete(key);
        } else {
            System.out.printf("Updating Key: %s", key);
            stateStore.put(key, context.timestamp());
        }
        return null;
    }

    @Override
    public void close() {
    }
}
