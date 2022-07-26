package com.sproutsocial.nsq;

import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;

import java.io.IOException;

import static org.slf4j.LoggerFactory.getLogger;

@ThreadSafe
public class SingleNsqdBallenceStrategy extends BasePubSub implements BalanceStrategy {
    private static final Logger logger = getLogger(SingleNsqdBallenceStrategy.class);
    protected final ConnectionDetails daemon;
    private final Publisher parent;
    private int failoverDurationSecs = 10;

    public SingleNsqdBallenceStrategy(Client client, Publisher parent, String nsqd) {
        super(client);
        this.parent = parent;
        daemon = new ConnectionDetails(nsqd,
                this.parent,
                this.failoverDurationSecs,
                this);
    }

    @Override
    public PubConnection getConnection() {
        if (!daemon.makeReady()) {
            logger.warn("We aren't able to connect just now, so we are going to sleep for {} seconds", failoverDurationSecs);
            Util.sleepQuietly(failoverDurationSecs * 1000);
        }
        if (daemon.makeReady())
            return daemon.con;
        else {
            throw new NSQException("Unable to connect");
        }
    }

    @Override
    public void lastPublishFailed() {
        daemon.markFailure();
    }

    @Override
    public synchronized void connectionClosed(PubConnection closedCon) {
        if (daemon.con == closedCon) {
            daemon.con = null;
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
        this.daemon.setFailoverDurationSecs(failoverDurationSecs);
    }


    @Override
    public String toString() {
        return "SingleNsqdBallenceStrategy{" +
                "daemon=" + daemon +
                ", failoverDurationSecs=" + failoverDurationSecs +
                '}';
    }
}
