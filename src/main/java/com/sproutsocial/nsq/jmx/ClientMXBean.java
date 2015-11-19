package com.sproutsocial.nsq.jmx;

public interface ClientMXBean {

    int getCurrentConnectionCount();

    int getTotalDisconnections();

    long getPublishedCount();

    long getPublishedFailoverCount();

    long getPublishFailedCount();

    int getFailedMessageCount();

    int getHandlerErrorCount();

}
