package com.sproutsocial.nsq;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.sproutsocial.nsq.Util.checkArgument;
import static com.sproutsocial.nsq.Util.checkNotNull;

@ThreadSafe
public class Publisher extends BasePubSub {
    private final PublisherConnectionPool pool;
    private final PublisherBalanceStrategy balanceStrategy;
    private final Map<String, Batcher> batchers = new HashMap<String, Batcher>();
    private ScheduledExecutorService batchExecutor;
    private int failoverDurationSecs = 300;

    private static final int DEFAULT_MAX_BATCH_SIZE = 16 * 1024;
    private static final int DEFUALT_MAX_BATCH_DELAY = 300;

    private static final Logger logger = LoggerFactory.getLogger(Publisher.class);

    public Publisher(Client client, List<HostAndPort> nsqds, PublisherBalanceStrategy balanceStrategy) {
        super(client);
        this.pool = new PublisherConnectionPool(client, this, nsqds);
        this.balanceStrategy = balanceStrategy;
        client.addPublisher(this);
    }

    public Publisher(Client client, String nsqd, String failoverNsqd) {
        this(client, nsqdsFromFailover(nsqd, failoverNsqd), new FailoverBalanceStrategy());
    }

    public Publisher(String nsqd, String failoverNsqd) {
        this(Client.getDefaultClient(), nsqd, failoverNsqd);
    }

    public Publisher(String nsqd) {
        this(nsqd, null);
    }

    public synchronized void publish(String topic, byte[] data) {
        checkNotNull(topic);
        checkNotNull(data);
        checkArgument(data.length > 0);

        withConnection(conn -> {
                try {
                    conn.publish(topic, data);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
    }

    public synchronized void publishDeferred(String topic, byte[] data, long delay, TimeUnit unit) {
        checkNotNull(topic);
        checkNotNull(data);
        checkArgument(data.length > 0);
        checkArgument(delay > 0);
        checkNotNull(unit);

        withConnection(conn -> {
                try {
                    conn.publishDeferred(topic, data, unit.toMillis(delay));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
    }

    public synchronized void publish(String topic, List<byte[]> dataList) {
        checkNotNull(topic);
        checkNotNull(dataList);
        checkArgument(dataList.size() > 0);

        withConnection(conn -> {
                try {
                    conn.publish(topic, dataList);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
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
        pool.stop();
        if (batchExecutor != null) {
            Util.shutdownAndAwaitTermination(batchExecutor, 40, TimeUnit.MILLISECONDS);
        }
        if (client.isLonePublisher(this)) { // convenience, prevents needing to call client.stop() to stop all threads
            Util.shutdownAndAwaitTermination(client.getSchedExecutor(), 40, TimeUnit.MILLISECONDS);
        }
    }

    public synchronized int getFailoverDurationSecs() {
        return failoverDurationSecs;
    }

    public synchronized void setFailoverDurationSecs(int failoverDurationSecs) {
        this.failoverDurationSecs = failoverDurationSecs;
    }

    private void withConnection(Consumer<PubConnection> fn) {
        pool.startIfStopped(config);

        PubConnection connection = null;
        while (true) {
            connection = balanceStrategy.getConnectionFrom(pool)
                .orElseThrow(() -> new NSQException("All publisher connections exhausted, failing to publish message"));
            try {
                fn.accept(connection);
                return;
            } catch (Exception e) {
                if (connection != null) {
                    connection.failConnectionFor(failoverDurationSecs);
                    logger.info("Marking connection as failed and trying the next available: {}", connection);
                }
            }
        }
    }

    private static final List<HostAndPort> nsqdsFromFailover(String nsqd, String failoverNsqd) {
        final ArrayList<HostAndPort> nsqds = new ArrayList<>();
        nsqds.add(HostAndPort.fromString(nsqd).withDefaultPort(4150));
        if (failoverNsqd != null) {
            nsqds.add(HostAndPort.fromString(failoverNsqd).withDefaultPort(4150));
        }
        return nsqds;
    }
}
