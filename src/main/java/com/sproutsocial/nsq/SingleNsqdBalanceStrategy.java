package com.sproutsocial.nsq;

import org.slf4j.Logger;

import java.util.concurrent.TimeUnit;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * A single NSQD balance strategy will attempt to publish to the single known NSQD.  If that fails on a first attempt
 * it will *block* for the failover duration (default 10 seconds) and attempt to reconnect.
 */
public class SingleNsqdBalanceStrategy extends BasePubSub implements BalanceStrategy {
    private static final Logger logger = getLogger(SingleNsqdBalanceStrategy.class);
    protected final NsqdInstance nsqdInstance;
    private int failoverDurationSecs = 10;

    public SingleNsqdBalanceStrategy(Client client, Publisher parent, String nsqd) {
        super(client);
        logger.warn("You are configured to use a singe NSQD balance strategy.  This has both availability and correctness issues.  " +
                "Nsq-j is sleeping for failover duration on a failed publish inside a critical lock section impacting all threads calling to publish.  " +
                "This may appear to be lock starvation. " +
                "The client is also not resilient to failures in this mode, a single outage can result in dataloss and crashing (slowly).  "+
                "Please use failover or round robin balance strategy to avoid these issues");
        nsqdInstance = new NsqdInstance(nsqd,
                parent,
                this.failoverDurationSecs,
                this);
    }

    @Override
    public NsqdInstance getNsqdInstance() {
        if (!nsqdInstance.makeReady()) {
            logger.warn("We aren't able to connect just now, so we are going to sleep for {} seconds", failoverDurationSecs);
            Util.sleepQuietly(TimeUnit.SECONDS.toMillis(failoverDurationSecs));
            if (nsqdInstance.makeReady())
                return nsqdInstance;
            else {
                throw new NSQException("Unable to connect");
            }
        } else {
            return nsqdInstance;
        }

    }

    @Override
    public synchronized void connectionClosed(PubConnection closedCon) {
        if (nsqdInstance.getCon() == closedCon) {
            nsqdInstance.clearConnection();
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
        this.nsqdInstance.setFailoverDurationSecs(failoverDurationSecs);
    }


    @Override
    public String toString() {
        return "SingleNsqdBallenceStrategy{" +
                "daemon=" + nsqdInstance +
                ", failoverDurationSecs=" + failoverDurationSecs +
                '}';
    }
}
