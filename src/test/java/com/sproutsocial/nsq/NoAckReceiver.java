package com.sproutsocial.nsq;

public class NoAckReceiver extends TestMessageHandler {
    private final long sleepMillis;

    public NoAckReceiver(long sleepMillis) {
        super((int) (sleepMillis*2.5));
        this.sleepMillis = sleepMillis;
    }

    @Override
    public void accept(Message msg) {
        receivedMessages.add((NSQMessage) msg);
    }
}
