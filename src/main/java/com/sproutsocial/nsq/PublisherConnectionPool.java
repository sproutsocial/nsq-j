package com.sproutsocial.nsq;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Manages connection state and pooling for nsqd instances for publishers. Fed
 * into a {@link com.sproutsocial.nsq.BalanceStrategy}, which is responsible for
 * connection selection at message publishing time.
 *
 * Each {@link com.sproutsocial.nsq.PubConnection} is pinned to a single publisher,
 * and PubConnection access is all thread-safe.
 */
public class PublisherConnectionPool {
    private final Client client;
    private final Publisher publisher;
    private final List<HostAndPort> nsqdHosts;
    private final List<PubConnection> connections;

    public PublisherConnectionPool(final Client client,
                                   final Publisher publisher,
                                   final List<HostAndPort> nsqdHosts) {
        this.client = client;
        this.publisher = publisher;
        this.nsqdHosts = nsqdHosts;
        this.connections = new ArrayList<>();
    }

    public int getHealthyConnectionCount() {
        return (int)connections.stream()
            .filter(conn -> conn.getConnectionState() == Connection.State.ESTABLISHED)
            .count();
    }

    public Optional<PubConnection> getHealthyConnectionAt(final int index) {
        return connections.stream()
            .filter(conn -> conn.getConnectionState() == Connection.State.ESTABLISHED)
            .skip(index)
            .findFirst();
    }

    public void stop() {
    }
}
