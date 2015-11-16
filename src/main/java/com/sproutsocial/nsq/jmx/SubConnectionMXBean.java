package com.sproutsocial.nsq.jmx;

public interface SubConnectionMXBean extends ConnectionMXBean {

    int getMaxInFlight();

    void stop();

    int getMaxAttemps();

    int getMaxFlushDelayMillis();

    int getInFlight();

    int getMaxUnflushed();

    long getRequeuedCount();

    long getFinishedCount();

}
