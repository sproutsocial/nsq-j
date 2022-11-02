package com.sproutsocial.nsq;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.sproutsocial.nsq.Util.checkNotNull;
import static org.slf4j.LoggerFactory.getLogger;

public class ListBasedBalanceStrategy extends BasePubSub implements BalanceStrategy {
    private static final Logger logger = getLogger(ListBasedBalanceStrategy.class);
    protected final List<ConnectionDetails> daemonList;
    private final Publisher parent;
    private final Function<List<ConnectionDetails>, ConnectionDetails> connectionDetailsSelector;
    private int failoverDurationSecs = 300;

    /**
     * Create a list based failover strategy that will alternate between all connected nsqd.  Will reconnect to a
     * disconnected or failed nsqd on the next publish that could be routed to that nsqd after the failoverDuration has
     * expired (Default 5 min).
     * <p>
     * This will throw an NSQD exception if all nsqd are in a failed state.
     *
     * @param nsqd a list of strings that represent HostAndPort objects.
     */
    public static BiFunction<Client, Publisher, BalanceStrategy> getRoundRobinStrategyBuilder(List<String> nsqd) {
        return (c, p) -> buildRoundRobinStrategy(c, p, nsqd);
    }

    /**
     * Create a list based failover strategy that shows strong preference to the first nsqd on the list.
     * <p>
     * On publish, find the first nsqd in this list that is in a connected or connectable state.  A nsqd is connectable
     * if it has previously failed more than the configured failoverDuration (Default 5 min).
     * <p>
     * This will throw an NSQD exception if all nsqd are in a failed state.
     *
     * @param nsqd a list of strings that represent HostAndPort objects.
     */
    public static BiFunction<Client, Publisher, BalanceStrategy> getFailoverStrategyBuilder(List<String> nsqd) {
        return (c, p) -> buildFailoverStrategy(c,p,nsqd);
    }

    private static ListBasedBalanceStrategy buildRoundRobinStrategy(Client client, Publisher parent, List<String> nsqd) {
        return new ListBasedBalanceStrategy(client, parent, nsqd, new Function<List<ConnectionDetails>, ConnectionDetails>() {
            private int nextDaemonIndex = 0;

            @Override
            public ConnectionDetails apply(List<ConnectionDetails> daemonList) {
                for (int attempts = 0; attempts < daemonList.size(); attempts++) {
                    ConnectionDetails candidate = daemonList.get(nextDaemonIndex);
                    boolean candidateReady = candidate.makeReady();
                    nextDaemonIndex++;
                    if (nextDaemonIndex >= daemonList.size()) {
                        nextDaemonIndex = 0;
                    }
                    if (candidateReady) {
                        return candidate;
                    }
                }
                // We've gotten to the point where all connections have been marked as 'failed'. Rather than intentionally
                // dropping messages on the floor, let's at least attempt to reconnect for subsequent message publishing.
                clearAllConnections(daemonList);
                throw new NSQException("publish failed: Unable to establish a connection with any NSQ host: " + daemonList);
            }
        });
    }

    private static ListBasedBalanceStrategy buildFailoverStrategy(Client client, Publisher parent, List<String> nsqd) {
        return new ListBasedBalanceStrategy(client, parent, nsqd, daemonList -> {
            for (int attempts = 0; attempts < daemonList.size(); attempts++) {
                ConnectionDetails candidate = daemonList.get(attempts);
                if (candidate.makeReady()) {
                    return candidate;
                }
            }
            // We've gotten to the point where all connections have been marked as 'failed'. Rather than intentionally
            // dropping messages on the floor, let's at least attempt to reconnect for subsequent message publishing.
            clearAllConnections(daemonList);
            throw new NSQException("publish failed: Unable to establish a connection with any NSQ host: " + daemonList);
        });
    }

    private static void clearAllConnections(final List<ConnectionDetails> daemonList) {
	for (final ConnectionDetails daemon : daemonList) {
	    daemon.clearConnection();
	}
    }

    public ListBasedBalanceStrategy(Client client, Publisher parent, List<String> nsqd, Function<List<ConnectionDetails>, ConnectionDetails> connectionDetailsSelector) {
        super(client);
        checkNotNull(parent);
        checkNotNull(nsqd);
        checkNotNull(connectionDetailsSelector);

        this.parent = parent;
        this.connectionDetailsSelector = connectionDetailsSelector;
        List<ConnectionDetails> connectionDetails = new ArrayList<>();
        for (String host : nsqd) {
            if (host != null)
                connectionDetails.add(new ConnectionDetails(host, this.parent, this.failoverDurationSecs, this));
        }
        daemonList = Collections.unmodifiableList(connectionDetails);
    }

    @Override
    public ConnectionDetails getConnectionDetails() {
        return connectionDetailsSelector.apply(daemonList);
    }

    @Override
    public synchronized void connectionClosed(PubConnection closedCon) {
        for (ConnectionDetails daemon : daemonList) {
            if (daemon.getCon() == closedCon) {
                daemon.clearConnection();
                logger.debug("removed closed publisher connection:{}", closedCon.getHost());
            }
        }
    }

    @Override
    public int getFailoverDurationSecs() {
        return this.failoverDurationSecs;
    }

    @Override
    public void setFailoverDurationSecs(int failoverDurationSecs) {
        this.failoverDurationSecs = failoverDurationSecs;
        for (ConnectionDetails connectionDetails : daemonList) {
            connectionDetails.setFailoverDurationSecs(failoverDurationSecs);
        }
    }


    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "{" + "daemonList=" + daemonList + ", failoverDurationSecs=" + failoverDurationSecs + '}';
    }
}
