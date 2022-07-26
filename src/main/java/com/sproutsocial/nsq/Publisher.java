package com.sproutsocial.nsq;

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
    private static final int DEFAULT_MAX_BATCH_SIZE = 16 * 1024;
    private static final int DEFUALT_MAX_BATCH_DELAY = 300;
    private static final Logger logger = LoggerFactory.getLogger(Publisher.class);
    private final BalanceStrategy balanceStrategy;
    private final Map<String, Batcher> batchers = new HashMap<>();
    private ScheduledExecutorService batchExecutor;

    public Publisher(Client client, String nsqd, String failoverNsqd) {
        super(client);
        client.addPublisher(this);
        balanceStrategy = BalanceStrategy.build(nsqd, failoverNsqd, this, client);
    }

    public Publisher(String nsqd, String failoverNsqd) {
        this(Client.getDefaultClient(), nsqd, failoverNsqd);
    }

    public Publisher(String nsqd) {
        this(nsqd, null);
    }


    public synchronized void connectionClosed(PubConnection closedCon) {
        balanceStrategy.connectionClosed(closedCon);
    }

    public synchronized void publish(String topic, byte[] data) {
        checkNotNull(topic);
        checkNotNull(data);
        checkArgument(data.length > 0);
        PubConnection connection = balanceStrategy.getConnection();
        try {
            connection.publish(topic, data);
        } catch (Exception e) {
            balanceStrategy.lastPublishFailed();
            logger.error("publish error with", e);
            publish(topic, data);
        }
    }

    public synchronized void publishDeferred(String topic, byte[] data, long delay, TimeUnit unit) {
        checkNotNull(topic);
        checkNotNull(data);
        checkArgument(data.length > 0);
        checkArgument(delay > 0);
        checkNotNull(unit);
        try {
            balanceStrategy.getConnection().publishDeferred(topic, data, unit.toMillis(delay));
        } catch (Exception e) {
            //deferred publish never fails over
            throw new NSQException("deferred publish failed", e);
        }
    }

    public synchronized void publish(String topic, List<byte[]> dataList) {
        checkNotNull(topic);
        checkNotNull(dataList);
        checkArgument(dataList.size() > 0);
        try {
            balanceStrategy.getConnection().publish(topic, dataList);
        } catch (Exception e) {
            logger.error("publish error", e);
            publish(topic, dataList);
        }
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
    }

    public synchronized int getFailoverDurationSecs() {
        return balanceStrategy.getFailoverDurationSecs();
    }

    public synchronized void setFailoverDurationSecs(int failoverDurationSecs) {
        balanceStrategy.setFailoverDurationSecs(failoverDurationSecs);
    }

}
