package org.example.transformer;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.example.wrapper.ValueWrapper;

import java.time.Duration;
import java.util.GregorianCalendar;

public class TTLKTableTombstoneEmitter<K,V,R> implements Transformer<K,V,R> {
    private ProcessorContext context;
    private KeyValueStore<K, Long> purgeStateStore;
    private final Duration maxAge;
    private final Duration scanFrequency;
    private final String purgeStateStoreName;

    public TTLKTableTombstoneEmitter(
            final Duration maxAge, final Duration scanFrequency, final String purgeStateStoreName
    ) {
        this.maxAge = maxAge;
        this.scanFrequency = scanFrequency;
        this.purgeStateStoreName = purgeStateStoreName;
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        purgeStateStore = this.context.getStateStore(purgeStateStoreName);
        context.schedule(scanFrequency, PunctuationType.WALL_CLOCK_TIME, this::wallClockPunctuator);
    }

    private void wallClockPunctuator(long timestamp) {
        long cutoff = timestamp - maxAge.toMillis();
        try( final KeyValueIterator<K, Long> iter = purgeStateStore.all()) {
            while(iter.hasNext()) {
                KeyValue<K, Long> keyValue = iter.next();
                System.out.println("RECORD "+keyValue.key+":"+keyValue.value);

                if (keyValue.value != null && keyValue.value < cutoff) {

                    System.out.println("Forwarding Null for key " + keyValue.key);
                    ValueWrapper vw = new ValueWrapper();
                    vw.setDeleted(true);
                    context.forward(keyValue.key, vw);
                    purgeStateStore.delete(keyValue.key);
                }
            }
        }
    }

    @Override
    public R transform(K k, V v) {
        if(v == null) {
            System.out.println("CLEANING KEY: "+k);
            purgeStateStore.put(k, null);
        } else {
            System.out.println("UPDATING KEY: "+k);
            purgeStateStore.put(k, context.timestamp());
        }
        return null;
    }

    @Override
    public void close() {
    }
}
