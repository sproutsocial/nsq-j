package com.sproutsocial.nsq.jmx;

public interface ConnectionMXBean {

    int getMsgTimeout();

    long getLastActionFlush();

    int getMaxRdyCount();

    int getHeartbeatInterval();

    int getUnflushedCount();

    long getLastHeartbeat();

}
