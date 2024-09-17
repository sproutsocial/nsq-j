package com.sproutsocial.nsq;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

class NSQMessage implements Message {

    private final long timestamp;
    private final int attempts;
    private final String id;
    private final byte[] data;
    private final String topic;
    private final SubConnection connection;
    private final AtomicBoolean responded = new AtomicBoolean();

    NSQMessage(long timestamp, int attempts, String id, byte[] data, String topic, SubConnection connection) {
        this.timestamp = timestamp;
        this.attempts = attempts;
        this.id = id;
        this.data = data;
        this.topic = topic;
        this.connection = connection;
    }

    @Override
    public byte[] getData() {
        return data;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public int getAttempts() {
        return attempts;
    }

    @Override
    public String getTopic() {
        return topic;
    }

    @Override
    public void finish() {
        if (responded.compareAndSet(false, true)) {
            connection.finish(id);
        }
    }

    @Override
    public void requeue() {
        requeue(0);
    }

    @Override
    public void requeue(int delayMillis) {
        if (responded.compareAndSet(false, true)) {
            connection.requeue(id, delayMillis);
        }
    }

    @Override
    public void touch() {
        if (responded.get()) {
            return;
        }
        connection.touch(id);
    }

    @Override
    public void forceFlush() {
        try {
            connection.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean hasResponded() {
        return responded.get();
    }

    SubConnection getConnection() {
        return connection;
    }
}
