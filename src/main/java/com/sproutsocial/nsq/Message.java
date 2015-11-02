package com.sproutsocial.nsq;

public interface Message {

    byte[] getData();

    String getId();

    int getAttempts();

    long getTimestamp();

    void finish();

    void requeue();

    void touch();

}
