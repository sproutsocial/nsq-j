package com.sproutsocial.nsq;

import java.util.List;
import java.util.concurrent.TimeUnit;

public interface PublisherInterface {
    void connectionClosed(PubConnection closedCon);

    void publish(String topic, byte[] data);

    void publishDeferred(String topic, byte[] data, long delay, TimeUnit unit);

    void publish(String topic, List<byte[]> dataList);

    void stop();

    int getFailoverDurationSecs();

    void setFailoverDurationSecs(int failoverDurationSecs);

}
