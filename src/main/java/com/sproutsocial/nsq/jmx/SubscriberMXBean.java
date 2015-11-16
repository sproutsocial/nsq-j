package com.sproutsocial.nsq.jmx;

import java.util.List;

public interface SubscriberMXBean {

    List<String> getLookups();

    List<String> getSubscriptions();

    int getMaxInFlightPerSubscription();

    void setMaxInFlightPerSubscription(int maxInFlightPerSubscription);

    int getMaxFlushDelayMillis();

    void setMaxFlushDelayMillis(int maxFlushDelayMillis);

    int getMaxAttempts();

    void setMaxAttempts(int maxAttempts);

    int getLookupIntervalSecs();

    Integer getExecutorQueueSize();

    void stop();

}
