package com.sproutsocial.nsq;

import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

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
        this(client, getBalanceStrategyBiFunction(nsqd, failoverNsqd));
    }

    private static BiFunction<Client, Publisher, BalanceStrategy> getBalanceStrategyBiFunction(String nsqd, String failoverNsqd) {
        Objects.requireNonNull(nsqd);

        if (failoverNsqd == null) {
            return (c, p) -> new SingleNsqdBallenceStrategy(c, p, nsqd);
        } else {
            return (c, p) -> ListBasedBalanceStrategy.buildFailoverStrategy(c, p, Arrays.asList(nsqd, failoverNsqd));
        }
    }

    public Publisher(Client client, BiFunction<Client, Publisher, BalanceStrategy> balanceStrategyFactory) {
        super(client);
        client.addPublisher(this);
        this.balanceStrategy = balanceStrategyFactory.apply(client, this);
    }


    public Publisher(String nsqd, String failoverNsqd) {
        this(Client.getDefaultClient(), nsqd, failoverNsqd);
    }

    public Publisher(String nsqd) {
        this(Client.getDefaultClient(), nsqd, null);
    }


    public synchronized void connectionClosed(PubConnection closedCon) {
        balanceStrategy.connectionClosed(closedCon);
    }

    public synchronized void publish(String topic, byte[] data) {
        checkNotNull(topic);
        checkNotNull(data);
        checkArgument(data.length > 0);
        ConnectionDetails connectionDetails = balanceStrategy.getConnectionDetails();
        try {
            connectionDetails.getCon().publish(topic, data);
        } catch (Exception e) {
            connectionDetails.markFailure();
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
        ConnectionDetails connection = balanceStrategy.getConnectionDetails();
        try {
            connection.getCon().publishDeferred(topic, data, unit.toMillis(delay));
        } catch (Exception e) {
            connection.markFailure();
            //deferred publish does not retry
            throw new NSQException("deferred publish failed", e);
        }
    }

    public synchronized void publish(String topic, List<byte[]> dataList) {
        checkNotNull(topic);
        checkNotNull(dataList);
        checkArgument(dataList.size() > 0);
        ConnectionDetails connectionDetails = balanceStrategy.getConnectionDetails();
        try {
            connectionDetails.getCon().publish(topic, dataList);
        } catch (Exception e) {
            logger.error("publish error", e);
            connectionDetails.markFailure();
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
        flushBatchers();
        super.stop();
        if (batchExecutor != null) {
            Util.shutdownAndAwaitTermination(batchExecutor, 40, TimeUnit.MILLISECONDS);
        }
        if (client.isLonePublisher(this)) { // convenience, prevents needing to call client.stop() to stop all threads
            Util.shutdownAndAwaitTermination(client.getSchedExecutor(), 40, TimeUnit.MILLISECONDS);
        }
    }

    protected void flushBatchers() {
        for (Batcher batcher : batchers.values()) {
            batcher.sendBatch();
        }
    }

    public synchronized int getFailoverDurationSecs() {
        return balanceStrategy.getFailoverDurationSecs();
    }

    public synchronized void setFailoverDurationSecs(int failoverDurationSecs) {
        balanceStrategy.setFailoverDurationSecs(failoverDurationSecs);
    }

}
