package com.sproutsocial.nsq;

import java.util.Optional;

public interface PublisherBalanceStrategy {
    Optional<PubConnection> getConnectionFrom(PublisherConnectionPool pool);
}
