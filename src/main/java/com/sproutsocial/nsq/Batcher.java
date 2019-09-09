package com.sproutsocial.nsq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.sproutsocial.nsq.Util.checkArgument;
import static com.sproutsocial.nsq.Util.checkNotNull;

class Batcher {

    private final Publisher publisher;
    private final String topic;
    private final int maxSize;
    private final int maxDelayMillis;
    private final boolean asyncPublish;
    private final ScheduledExecutorService executor;
    private int size;
    private List<byte[]> batch = new ArrayList<byte[]>();
    private long sendTime;

    private static final Logger logger = LoggerFactory.getLogger(Batcher.class);

    public Batcher(Publisher publisher, String topic, int maxSizeBytes, int maxDelayMillis, boolean asyncPublish) {
        this.publisher = publisher;
        this.topic = topic;
        this.maxSize = maxSizeBytes;
        this.maxDelayMillis = maxDelayMillis;
        this.executor = publisher.getBatchExecutor();
        this.asyncPublish = asyncPublish;
        checkNotNull(publisher);
        checkNotNull(topic);
        checkArgument(maxDelayMillis > 5);
        checkArgument(maxDelayMillis <= 60000);
        checkArgument(maxSize > 100);
    }

    public Batcher(Publisher publisher, String topic, int maxSizeBytes, int maxDelayMillis) {
        this(publisher, topic, maxSizeBytes, maxDelayMillis, false);
    }

    public void publish(byte[] msg) {
        boolean sendNow = false;
        synchronized (this) {
            batch.add(msg);
            size += msg.length;
            if (batch.size() == 1) {
                sendTime = Util.clock() + maxDelayMillis;
                executor.schedule(new Runnable() {
                    public void run() {
                        sendDelayedBatch();
                    }
                }, maxDelayMillis, TimeUnit.MILLISECONDS);
            }
            else if (size >= maxSize) {
                sendNow = true;
            }
        }
        if (sendNow) {
            sendBatch();
        }
    }

    private void sendDelayedBatch() {
        try {
            boolean sendNow = false;
            synchronized (this) {
                if (!batch.isEmpty()) {
                    long delay = sendTime - Util.clock();
                    if (delay < 50) {
                        sendNow = true;
                    }
                    else {
                        executor.schedule(new Runnable() {
                            public void run() {
                                sendDelayedBatch();
                            }
                        }, delay, TimeUnit.MILLISECONDS);
                    }
                }
            }
            if (sendNow) {
                sendBatch();
            }
        }
        catch (Throwable t) {
            logger.error("delayed batch error. messages possibly lost", t);
        }
    }

    void sendBatch() {
        if (asyncPublish) {
            sendBatchAsync();
        } else {
            sendBatchSync();
        }
    }

    /**
     * If you're going to use async publishing, make sure you set aggressive timeouts, to avoid too much
     * stalls and lock contention.
     */
    private void sendBatchAsync() {
        executor.submit(new Runnable() {
                public void run() {
                    sendBatchSync();
                }
            });
    }

    private void sendBatchSync() {
        List<byte[]> toSend = null;
        synchronized (this) {
            if (!batch.isEmpty()) {
                toSend = batch;
                int capacity = Math.min(maxSize, (int) (toSend.size() * 1.2));
                capacity = Math.max(10, capacity);
                batch = new ArrayList<byte[]>(capacity);
                size = 0;
            }
        }
        if (toSend != null) {
            publisher.publish(topic, toSend);
        }
    }

}
