package com.sproutsocial.nsq;

import org.junit.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TestMessageHandler implements MessageHandler {
    public static final int DEFAULT_TIMEOUT_MILLIS = 5000;
    public final int timeoutMillis;
    BlockingQueue<NSQMessage> receivedMessages = new LinkedBlockingQueue<>();

    public  TestMessageHandler() {
        this(DEFAULT_TIMEOUT_MILLIS);
    }

    public TestMessageHandler(int timeoutMillis) {
        this.timeoutMillis = timeoutMillis;
    }

    @Override
    public void accept(Message msg) {
        receivedMessages.add((NSQMessage) msg);
        msg.finish();
        msg.forceFlush();
    }

    public List<NSQMessage> drainMessagesOrTimeOut(int size) {
        return drainMessagesOrTimeOut(size, timeoutMillis);
    }

    public List<NSQMessage> drainMessagesOrTimeOut(int size, int timeoutMillis) {
        long timeoutTime = System.currentTimeMillis() + timeoutMillis;
        while (receivedMessages.size() < size && System.currentTimeMillis() < timeoutTime) {
            Util.sleepQuietly(50);
        }
        if (System.currentTimeMillis() > timeoutTime) {
            Assert.fail("Timed out waiting for messages.  Received " + receivedMessages.size() + " out of expected " + size);
        }
        List<NSQMessage> drained = new ArrayList<>();
        receivedMessages.drainTo(drained);
        return drained;
    }

    public List<NSQMessage> drainMessages(int size) {
        List<NSQMessage> drained = new ArrayList<>();
        receivedMessages.drainTo(drained, size);
        return drained;
    }
}
