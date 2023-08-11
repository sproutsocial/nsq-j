package com.sproutsocial.nsq;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class SubscriberFocusedDockerTestIT extends BaseDockerTestIT {
    private Publisher publisher;
    private List<Subscriber> subscribers = new ArrayList<>();

    @Override
    public void setup() {
        super.setup();
        publisher = this.backupPublisher();
    }

    @Test
    public void twoDifferentSubscribersShareMessages() {
        TestMessageHandler handler1 = new TestMessageHandler();
        TestMessageHandler handler2 = new TestMessageHandler();
        startSubscriber(handler1, "channelA", null);
        startSubscriber(handler2, "channelA", null);
        List<String> messages = messages(20, 40);

        send(topic, messages, 1, 200, publisher);

        Util.sleepQuietly(1000);

        List<NSQMessage> firstConsumerMessages = handler1.drainMessages(20);
        List<NSQMessage> secondConsumerMessages = handler2.drainMessages(20);
        Assert.assertFalse("Expect first consumer to have received some messages", firstConsumerMessages.isEmpty());
        Assert.assertFalse("Expect second consumer to have received some messages", secondConsumerMessages.isEmpty());

        List<NSQMessage> combined = new ArrayList<>(firstConsumerMessages);
        combined.addAll(secondConsumerMessages);
        validateReceivedAllMessages(messages, combined, false);
    }

    @Test
    public void unsubscribingSubscribers() {
        TestMessageHandler handler = new TestMessageHandler();
        Subscriber subscriber = startSubscriber(handler, "channelA", null);
        List<String> batch1 = messages(20, 40);
        List<String> batch2 = messages(20, 40);

        send(topic, batch1, 0, 0, publisher);
        Util.sleepQuietly(1000);
        // Unsubscribe after the first batch.
        Assert.assertTrue(subscriber.unsubscribe(topic, "channelA"));
        send(topic, batch2, 0, 0, publisher);

        Util.sleepQuietly(1000);

        // Ensure we only get 20 messages, even though we sent 40.
        List<NSQMessage> consumerMessages = handler.drainMessages(20);
        Assert.assertEquals(20, consumerMessages.size());
    }

    @Test
    public void verySlowConsumer_allMessagesReceivedByResponsiveConsumer() {
        TestMessageHandler handler = new TestMessageHandler();
        NoAckReceiver delayHandler = new NoAckReceiver(8000);
        startSubscriber(handler, "channelA", null);
        startSubscriber(delayHandler, "channelA", null);
        List<String> messages = messages(40, 40);

        send(topic, messages, 1, 100, publisher);

        List<NSQMessage> firstConsumerMessages = handler.drainMessagesOrTimeOut(40, 15000);
        List<NSQMessage> delayedMessages = delayHandler.drainMessages(40);
        Assert.assertFalse("Expect the consumer that doesn't ack to have received some messages", delayedMessages.isEmpty());

        validateReceivedAllMessages(messages, firstConsumerMessages, false);
    }


    @Override
    public void teardown() throws InterruptedException {
        publisher.stop();
        for (Subscriber subscriber : subscribers) {
            subscriber.stop();
        }
        subscribers.clear();
        super.teardown();
    }

    private Subscriber startSubscriber(TestMessageHandler handler, String channel, FailedMessageHandler failedMessageHandler) {
        Subscriber subscriber = new Subscriber(client, 1, 10, cluster.getLookupNode().getHttpHostAndPort().toString());
        subscriber.setDefaultMaxInFlight(1);
        subscriber.setMaxAttempts(5);
        if (failedMessageHandler != null) {
            subscriber.setFailedMessageHandler(failedMessageHandler);
        }
        subscriber.subscribe(topic, channel, handler);
        this.subscribers.add(subscriber);
        return subscriber;
    }
}
