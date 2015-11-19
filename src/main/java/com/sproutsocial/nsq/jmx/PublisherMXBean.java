package com.sproutsocial.nsq.jmx;

public interface PublisherMXBean {

    String getNsqd();

    String getFailoverNsqd();

    int getFailoverDurationSecs();

    void setFailoverDurationSecs(int failoverDurationSecs);

    boolean isFailover();

    boolean isConnected();

    long getPublishedCount();

    long getPublishedFailoverCount();

    long getPublishFailedCount();

    void stop();

}
