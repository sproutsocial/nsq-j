package com.sproutsocial.nsq.jmx;

public interface PublisherMXBean {

    String getNsqd();

    String getFailoverNsqd();

    int getFailoverDurationSecs();

    void setFailoverDurationSecs(int failoverDurationSecs);

    boolean isFailover();

    long getPublishedCount();

    long getPublishedFailoverCount();

    void stop();

}
