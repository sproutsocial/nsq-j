package com.sproutsocial.nsq;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.sproutsocial.nsq.Util.checkArgument;
import static com.sproutsocial.nsq.Util.checkNotNull;

@ThreadSafe
public class FailoverPublisher extends BasePubSub implements PublisherInterface {

    private final HostAndPort nsqd;
    private final HostAndPort failoverNsqd;
    private Publisher parent;
    private PubConnection con;
    private boolean isFailover = false;
    private long failoverStart;
    private int failoverDurationSecs = 300;

    private static final int DEFAULT_MAX_BATCH_SIZE = 16 * 1024;
    private static final int DEFUALT_MAX_BATCH_DELAY = 300;

    private static final Logger logger = LoggerFactory.getLogger(FailoverPublisher.class);

    public FailoverPublisher(Client client, String nsqd, String failoverNsqd, Publisher parent) {
        super(client);
        this.nsqd = HostAndPort.fromString(nsqd).withDefaultPort(4150);
        this.failoverNsqd = failoverNsqd != null ? HostAndPort.fromString(failoverNsqd).withDefaultPort(4150) : null;
        this.parent = parent;
    }


    @GuardedBy("this")
    private void checkConnection() throws IOException {
        if (con == null) {
            if (isStopping) {
                throw new NSQException("publisher stopped");
            }
            connect(nsqd);
        }
        else if (isFailover && Util.clock() - failoverStart > failoverDurationSecs * 1000) {
            isFailover = false;
            connect(nsqd);
            logger.info("using primary nsqd");
        }
    }

    @GuardedBy("this")
    private void connect(HostAndPort host) throws IOException {
        if (con != null) {
            con.close();
        }
        con = new PubConnection(client, host, parent);
        try {
            con.connect(config);
        }
        catch(IOException e) {
            con.close();
            con = null;
            throw e;
        }
        logger.info("publisher connected:{}", host);
    }

    @Override
    public synchronized void connectionClosed(PubConnection closedCon) {
        if (con == closedCon) {
            con = null;
            logger.debug("removed closed publisher connection:{}", closedCon.getHost());
        }
    }

    @Override
    public synchronized void publish(String topic, byte[] data) {
        checkNotNull(topic);
        checkNotNull(data);
        checkArgument(data.length > 0);
        try {
            checkConnection();
            con.publish(topic, data);
        }
        catch (Exception e) {
            logger.error("publish error with:{}", isFailover ? failoverNsqd : nsqd, e);
            publishFailover(topic, data);
        }
    }

    @Override
    public synchronized void publishDeferred(String topic, byte[] data, long delay, TimeUnit unit) {
        checkNotNull(topic);
        checkNotNull(data);
        checkArgument(data.length > 0);
        checkArgument(delay > 0);
        checkNotNull(unit);
        try {
            checkConnection();
            con.publishDeferred(topic, data, unit.toMillis(delay));
        }
        catch (Exception e) {
            //deferred publish never fails over
            throw new NSQException("deferred publish failed", e);
        }
    }

    @Override
    public synchronized void publish(String topic, List<byte[]> dataList) {
        checkNotNull(topic);
        checkNotNull(dataList);
        checkArgument(dataList.size() > 0);
        try {
            checkConnection();
            con.publish(topic, dataList);
        }
        catch (Exception e) {
            logger.error("publish error with:{}", isFailover ? failoverNsqd : nsqd, e);
            for (byte[] data : dataList) {
                publishFailover(topic, data);
            }
        }
    }

    @GuardedBy("this")
    private void publishFailover(String topic, byte[] data) {
        try {
            if (failoverNsqd == null) {
                logger.warn("publish failed but no failoverNsqd configured. Will wait and retry once.");
                Util.sleepQuietly(10000); //could do exponential backoff or make configurable
                connect(nsqd);
            }
            else if (!isFailover) {
                failoverStart = Util.clock();
                isFailover = true;
                connect(failoverNsqd);
                logger.info("using failover nsqd:{}", failoverNsqd);
            }
            con.publish(topic, data);
        }
        catch (Exception e) {
            Util.closeQuietly(con);
            con = null;
            isFailover = false;
            throw new NSQException("publish failed", e);
        }
    }


    @Override
    public synchronized void stop() {
        super.stop();
        Util.closeQuietly(con);
        con = null;
    }

    @Override
    public synchronized int getFailoverDurationSecs() {
        return failoverDurationSecs;
    }

    @Override
    public synchronized void setFailoverDurationSecs(int failoverDurationSecs) {
        this.failoverDurationSecs = failoverDurationSecs;
    }


}
