package com.sproutsocial.nsq;

import java.util.Optional;

/**
 * A publish balance strategy that always tries to publish to the first
 * healthy connection that's available, in descending order.
 */
public class FailoverBalanceStrategy implements PublisherBalanceStrategy {
    @Override
    public Optional<PubConnection> getConnectionFrom(PublisherConnectionPool pool) {
        return pool.getHealthyConnectionAt(0);
    }
}
