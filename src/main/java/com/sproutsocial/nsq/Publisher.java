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
public class Publisher extends BasePubSub {
    private final PublisherInterface delegate;

    private final Map<String, Batcher> batchers = new HashMap<String, Batcher>();
    private ScheduledExecutorService batchExecutor;

    private static final int DEFAULT_MAX_BATCH_SIZE = 16 * 1024;
    private static final int DEFUALT_MAX_BATCH_DELAY = 300;

    private static final Logger logger = LoggerFactory.getLogger(Publisher.class);

    /**
     *
     * @param client a Client object
     * @param nsqd Either a singular NSQD hostname and port in the form of "hostname:port" or
     *             a comma separated list of NSQD instances like "host1:123,host2:123".  If
     *             a comma separated list is provided, that will enable round robin publishing.
     *             Each call to a publish method will rotate to the next alive nsqd.  If you are
     *             round robin publishing, messages may be delivered out of order to downstream
     *             clients.
     * @param failoverNsqd Null or a singular NSQD hostname to use as a backup or a comma separated list
     *                     of more NSQD hosts to include in the round robin pool.  If a comma seperated
     *                     list is provided, round robin publishing will be enabled.
     */
    public Publisher(Client client, String nsqd, String failoverNsqd) {
        super(client);
        if (nsqd.contains(",") || (failoverNsqd != null && failoverNsqd.contains(","))) {
            delegate = new RoundRobinPublisher(client, nsqd, failoverNsqd, this);
        }else {
            delegate = new FailoverPublisher(client, nsqd, failoverNsqd, this);
        }
        client.addPublisher(this);
    }

    public Publisher(String nsqd, String failoverNsqd) {
        this(Client.getDefaultClient(), nsqd, failoverNsqd);
    }

    public Publisher(String nsqd) {
        this(nsqd, null);
    }


    public synchronized void connectionClosed(PubConnection closedCon) {
        delegate.connectionClosed(closedCon);
    }

    public synchronized void publish(String topic, byte[] data) {
        delegate.publish(topic, data);
    }

    public synchronized void publishDeferred(String topic, byte[] data, long delay, TimeUnit unit) {
        delegate.publishDeferred(topic, data, delay, unit);
    }

    public synchronized void publish(String topic, List<byte[]> dataList) {
        delegate.publish(topic, dataList);
    }

    public synchronized void publishBuffered(String topic, byte[] data) {
        checkNotNull(topic);
        checkNotNull(data);
        checkArgument(data.length > 0);
        Batcher batcher = batchers.get(topic);
        if (batcher == null) {
            batcher = new Batcher(this, topic, DEFAULT_MAX_BATCH_SIZE, DEFUALT_MAX_BATCH_DELAY);
            batchers.put(topic, batcher);
        }
        batcher.publish(data);
    }

    public synchronized void setBatchConfig(String topic, int maxSizeBytes, int maxDelayMillis) {
        Batcher batcher = batchers.get(topic);
        if (batcher != null) {
            batcher.sendBatch();
        }
        batcher = new Batcher(this, topic, maxSizeBytes, maxDelayMillis);
        batchers.put(topic, batcher);
    }

    synchronized ScheduledExecutorService getBatchExecutor() {
        if (batchExecutor == null) {
            batchExecutor = Executors.newScheduledThreadPool(1, Util.threadFactory("nsq-batch"));
        }
        return batchExecutor;
    }

    @Override
    public synchronized void stop() {
        for (Batcher batcher : batchers.values()) {
            batcher.sendBatch();
        }
        super.stop();
        if (batchExecutor != null) {
            Util.shutdownAndAwaitTermination(batchExecutor, 40, TimeUnit.MILLISECONDS);
        }
        if (client.isLonePublisher(this)) { // convenience, prevents needing to call client.stop() to stop all threads
            Util.shutdownAndAwaitTermination(client.getSchedExecutor(), 40, TimeUnit.MILLISECONDS);
        }
        delegate.stop();
    }

    public synchronized int getFailoverDurationSecs() {
        return delegate.getFailoverDurationSecs();
    }

    public synchronized void setFailoverDurationSecs(int failoverDurationSecs) {
        delegate.setFailoverDurationSecs(failoverDurationSecs);
    }

}
