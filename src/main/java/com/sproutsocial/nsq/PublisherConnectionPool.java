package com.sproutsocial.nsq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

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
    private static final Logger logger = LoggerFactory.getLogger(PublisherConnectionPool.class);

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
    public Optional<PubConnection> getHealthyConnectionAt(final long wrapAroundIndex) {
        final List<PubConnection> healthyConnections = getHealthyConnections();
        final int index = (int)(wrapAroundIndex % healthyConnections.size());
        return healthyConnections.stream()
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
        logger.info("nsq-j publisher connection pool starting");
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
        started = true;
    }

    @GuardedBy("Publisher")
    public void startIfStopped(Config config) {
        if (!started) {
            start(config);
        }
    }

    @GuardedBy("Publisher")
    private final List<PubConnection> getHealthyConnections() {
        return connections.stream()
            .map(conn -> {
                    conn.maybeAttemptRetry();
                    return conn;
                })
            .filter(conn -> conn.getConnectionState() == Connection.State.ESTABLISHED)
            .collect(Collectors.toList());
    }
}
