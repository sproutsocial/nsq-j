package com.sproutsocial.nsq;

import com.google.common.net.HostAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class Publisher extends BasePubSub {

    private final HostAndPort nsqd;
    private final HostAndPort failoverNsqd;
    private PubConnection con;
    private boolean isFailover = false;
    private long failoverStart;
    private int failoverDurationSecs = 300;

    private static final Logger logger = LoggerFactory.getLogger(Publisher.class);

    public Publisher(String nsqd, String failoverNsqd) {
        this.nsqd = HostAndPort.fromString(nsqd).withDefaultPort(4150);
        this.failoverNsqd = failoverNsqd != null ? HostAndPort.fromString(failoverNsqd).withDefaultPort(4150) : null;
        Client.addPublisher(this);
    }

    public Publisher(String nsqd) {
        this(nsqd, null);
    }

    private void checkConnection() throws IOException {
        if (con == null) {
            if (isStopping) {
                throw new NSQException("publisher stopped");
            }
            connect(nsqd);
        }
        else if (isFailover && Client.clock() - failoverStart > failoverDurationSecs * 1000) {
            connect(nsqd);
            isFailover = false;
            logger.info("using primary nsqd");
        }
    }

    private void connect(HostAndPort host) throws IOException {
        if (con != null) {
            con.close();
        }
        con = new PubConnection(host, this);
        con.connect(config);
        logger.info("publisher connected:{}", host);
    }

    public synchronized void connectionClosed(PubConnection closedCon) {
        if (con == closedCon) {
            con = null;
            logger.debug("removed closed publisher connection:{}", closedCon.getHost());
        }
    }

    public synchronized void publish(String topic, byte[] data) {
        try {
            checkConnection();
            con.publish(topic, data);
        }
        catch (Exception e) {
            logger.warn("publish error with:{} {}", isFailover ? failoverNsqd : nsqd, e);
            publishFailover(topic, data);
        }
    }

    public synchronized void publish(String topic, List<byte[]> dataList) {
        try {
            checkConnection();
            con.publish(topic, dataList);
        }
        catch (Exception e) {
            logger.warn("publish error with:{} {}", isFailover ? failoverNsqd : nsqd, e);
            for (byte[] data : dataList) {
                publishFailover(topic, data);
            }
        }
    }

    private void publishFailover(String topic, byte[] data) {
        try {
            if (failoverNsqd == null) {
                logger.warn("publish failed but no failoverNsqd configured. Will wait and retry once.");
                Util.sleepQuietly(10000); //could do exponential backoff or make configurable
                connect(nsqd);
            }
            else if (!isFailover) {
                failoverStart = Client.clock();
                isFailover = true;
                connect(failoverNsqd);
                logger.info("using failover nsqd:{}", failoverNsqd);
            }
            con.publish(topic, data);
        }
        catch (Exception e) {
            Util.closeQuietly(con);
            con = null;
            throw new NSQException("publish failed", e);
        }
    }

    @Override
    public synchronized void stop() {
        super.stop();
        Util.closeQuietly(con);
        con = null;
    }

    public synchronized int getFailoverDurationSecs() {
        return failoverDurationSecs;
    }

    public synchronized void setFailoverDurationSecs(int failoverDurationSecs) {
        this.failoverDurationSecs = failoverDurationSecs;
    }

}
