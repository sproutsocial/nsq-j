package com.sproutsocial.nsq;

import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

public class SingleNsqdBallenceStrategy extends BasePubSub implements BalanceStrategy {
    private static final Logger logger = getLogger(SingleNsqdBallenceStrategy.class);
    protected final ConnectionDetails connectionDetails;
    private final Publisher parent;
    private int failoverDurationSecs = 10;

    public SingleNsqdBallenceStrategy(Client client, Publisher parent, String nsqd) {
        super(client);
        this.parent = parent;
        connectionDetails = new ConnectionDetails(nsqd,
                this.parent,
                this.failoverDurationSecs,
                this);
    }

    @Override
    public ConnectionDetails getConnectionDetails() {
        if (!connectionDetails.makeReady()) {
            logger.warn("We aren't able to connect just now, so we are going to sleep for {} seconds", failoverDurationSecs);
            Util.sleepQuietly(failoverDurationSecs * 1000);
            if (connectionDetails.makeReady())
                return connectionDetails;
            else {
                throw new NSQException("Unable to connect");
            }
        } else {
            return connectionDetails;
        }

    }

    @Override
    public synchronized void connectionClosed(PubConnection closedCon) {
        if (connectionDetails.getCon() == closedCon) {
            connectionDetails.clearConnection();
            logger.debug("removed closed publisher connection:{}", closedCon.getHost());
        }
    }

    @Override
    public int getFailoverDurationSecs() {
        return failoverDurationSecs;
    }

    @Override
    public void setFailoverDurationSecs(int failoverDurationSecs) {
        this.failoverDurationSecs = failoverDurationSecs;
        this.connectionDetails.setFailoverDurationSecs(failoverDurationSecs);
    }


    @Override
    public String toString() {
        return "SingleNsqdBallenceStrategy{" +
                "daemon=" + connectionDetails +
                ", failoverDurationSecs=" + failoverDurationSecs +
                '}';
    }
}
