package com.sproutsocial.nsq;

import java.io.IOException;

class NSQMessage implements Message {

    private final long timestamp;
    private final int attempts;
    private final String id;
    private final byte[] data;
    private final SubConnection connection;

    NSQMessage(long timestamp, int attempts, String id, byte[] data, SubConnection connection) {
        this.timestamp = timestamp;
        this.attempts = attempts;
        this.id = id;
        this.data = data;
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
    public void finish() {
        try {
            connection.finish(id);
        }
        catch (IOException e) {
            throw new NSQException("msg finish error", e);
        }
    }

    @Override
    public void requeue() {
        try {
            connection.requeue(id);
        }
        catch (IOException e) {
            throw new NSQException("msg requeue error", e);
        }
    }

    @Override
    public void touch() {
        try {
            connection.touch(id);
        }
        catch (IOException e) {
            throw new NSQException("msg touch error", e);
        }
    }

}
