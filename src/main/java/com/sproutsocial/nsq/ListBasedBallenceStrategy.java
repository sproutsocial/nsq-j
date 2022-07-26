package com.sproutsocial.nsq;

import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.slf4j.LoggerFactory.getLogger;

@ThreadSafe
public abstract class ListBasedBallenceStrategy extends BasePubSub implements BalanceStrategy {
    private static final Logger logger = getLogger(ListBasedBallenceStrategy.class);
    protected final List<ConnectionDetails> daemonList;
    private Publisher parent;
    private int failoverDurationSecs = 300;
    private ConnectionDetails lastReturnedConnectionDetails;

    public ListBasedBallenceStrategy(Client client, Publisher parent, List<String> nsqd) {
        super(client);
        this.parent = parent;
        List<ConnectionDetails> connectionDetails = new ArrayList<>();
        for (String host : nsqd) {
            if (host != null)
                connectionDetails.add(new ConnectionDetails(host,
                        this.parent,
                        this.failoverDurationSecs,
                        this));
        }
        daemonList = Collections.unmodifiableList(connectionDetails);
    }

    @Override
    public PubConnection getConnection() {
        lastReturnedConnectionDetails = getCurrentConnectionDetails();
        return lastReturnedConnectionDetails.con;
    }

    @Override
    public void lastPublishFailed() {
        lastReturnedConnectionDetails.markFailure();
    }

    @Override
    public synchronized void connectionClosed(PubConnection closedCon) {
        for (ConnectionDetails daemon : daemonList) {
            if (daemon.con == closedCon) {
                daemon.con = null;
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

    public abstract ConnectionDetails getCurrentConnectionDetails();


    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "{" +
                "daemonList=" + daemonList +
                ", failoverDurationSecs=" + failoverDurationSecs +
                ", lastReturnedConnectionDetails=" + lastReturnedConnectionDetails +
                '}';
    }
}
