package com.sproutsocial.nsq;

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
        connection.finish(id);
    }

    @Override
    public void requeue() {
        connection.requeue(id);
    }

    @Override
    public void touch() {
        connection.touch(id);
    }

}
