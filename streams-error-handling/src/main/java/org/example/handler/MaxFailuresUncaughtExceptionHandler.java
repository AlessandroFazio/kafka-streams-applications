package org.example.handler;

import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;

public class MaxFailuresUncaughtExceptionHandler implements StreamsUncaughtExceptionHandler {
    private final int maxFailures;
    private final long maxTimeIntervalMillis;
    private int currentFailuresCount;
    private Instant previousErrorTime;

    public MaxFailuresUncaughtExceptionHandler(int maxFailures, long maxTimeIntervalMillis) {
        this.maxFailures = maxFailures;
        this.maxTimeIntervalMillis = maxTimeIntervalMillis;
    }

    @Override
    public StreamThreadExceptionResponse handle(Throwable throwable) {
        currentFailuresCount++;
        Instant currentErrorTime = Instant.now();

        if(previousErrorTime == null) {
            previousErrorTime = currentErrorTime;
        }

        long timeBetweenFailuresMillis =
                ChronoUnit.MILLIS.between(previousErrorTime, currentErrorTime);
        if(currentFailuresCount >= maxFailures) {
            if(timeBetweenFailuresMillis <= maxTimeIntervalMillis) {
                return SHUTDOWN_APPLICATION;
            } else {
                previousErrorTime = null;
                currentFailuresCount = 0;
            }
        }
        return REPLACE_THREAD;
    }
}
