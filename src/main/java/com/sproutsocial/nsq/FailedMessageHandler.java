package com.sproutsocial.nsq;

public interface FailedMessageHandler {

    void failed(String topic, String channel, Message msg);

}
