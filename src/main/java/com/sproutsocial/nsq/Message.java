package com.sproutsocial.nsq;

public interface Message {

    String getTopic();

    byte[] getData();

    String getId();

    int getAttempts();

    long getTimestamp();

    void finish();

    void requeue();

    void requeue(int delayMillis);

    void touch();

    void forceFlush();

}
