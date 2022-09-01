package com.sproutsocial.nsq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import net.jcip.annotations.GuardedBy;

/**
 * Manages connection state and pooling for nsqd instances for publishers. Fed
 * into a {@link com.sproutsocial.nsq.PublisherBalanceStrategy}, which is responsible for
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

    private boolean started;

    public PublisherConnectionPool(final Client client,
                                   final Publisher publisher,
                                   final List<HostAndPort> nsqdHosts) {
        this.client = client;
        this.publisher = publisher;
        this.nsqdHosts = nsqdHosts;
        this.connections = new ArrayList<>();
        this.started = false;
    }

    @GuardedBy("Publisher")
    public int getHealthyConnectionCount() {
        return (int)connections.stream()
            .filter(conn -> conn.getConnectionState() == Connection.State.ESTABLISHED)
            .count();
    }

    @GuardedBy("Publisher")
    public Optional<PubConnection> getHealthyConnectionAt(final int index) {
        return connections.stream()
            .map(conn -> {
                    if (conn.isReadyForRetry()) {
                        conn.retryConnection();
                    }
                    return conn;
                })
            .filter(conn -> conn.getConnectionState() == Connection.State.ESTABLISHED)
            .skip(index)
            .findFirst();
    }

    @GuardedBy("Publisher")
    public void stop() {
        for (final PubConnection connection : connections) {
            connection.close();
        }
    }

    @GuardedBy("Publisher")
    public void start(Config config) {
        for (final HostAndPort host : nsqdHosts) {
            final PubConnection connection = new PubConnection(client, host, publisher);
            try {
                connection.connect(config);
            } catch (IOException e) {
                final long failDuration = 5_000; // TODO: Make this configurable?
                connection.failConnectionFor(failDuration);
            }
            connections.add(connection);
        }
    }

    @GuardedBy("Publisher")
    public void startIfStopped(Config config) {
        if (!started) {
            start(config);
            started = true;
        }
    }
}
