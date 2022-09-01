package com.sproutsocial.nsq;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Balance strategy that attempts to hand out nsqd connections in a round-robin /
 * even fashion by cycling through connections that are healthy.
 */
public class RoundRobinBalanceStrategy implements PublisherBalanceStrategy {
    private AtomicLong counter;

    public RoundRobinBalanceStrategy() {
        this.counter = new AtomicLong(0L);
    }

    @Override
    public Optional<PubConnection> getConnectionFrom(PublisherConnectionPool pool) {
        final long index = counter.getAndIncrement();
        return pool.getHealthyConnectionAt(index);
    }
}
