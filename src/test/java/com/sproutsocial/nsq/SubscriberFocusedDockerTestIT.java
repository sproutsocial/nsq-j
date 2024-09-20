package com.sproutsocial.nsq;

import org.junit.Assert;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class SubscriberFocusedDockerTestIT extends BaseDockerTestIT {
    private static Logger logger = LoggerFactory.getLogger(SubscriberFocusedDockerTestIT.class);
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
        final Subscriber subscriber1 = startSubscriber(handler1, "channelA", null, null);
        final Subscriber subscriber2 = startSubscriber(handler2, "channelA", null, null);
        List<String> messages = messages(20, 40);

        send(topic, messages, 1, 200, publisher);

        Util.sleepQuietly(1000);

        List<NSQMessage> firstConsumerMessages = handler1.drainMessages(20);
        List<NSQMessage> secondConsumerMessages = handler2.drainMessages(20);
        awaitNoInFlightMessages(subscriber1);
        awaitNoInFlightMessages(subscriber2);
        Assert.assertFalse("Expect first consumer to have received some messages", firstConsumerMessages.isEmpty());
        Assert.assertFalse("Expect second consumer to have received some messages", secondConsumerMessages.isEmpty());

        List<NSQMessage> combined = new ArrayList<>(firstConsumerMessages);
        combined.addAll(secondConsumerMessages);
        validateReceivedAllMessages(messages, combined, false);
    }

    @Test
    public void unsubscribingSubscribers() {
        TestMessageHandler handler = new TestMessageHandler();
        AtomicReference<SubscriptionId> subscriptionId = new AtomicReference<>();
        Subscriber subscriber = startSubscriber(handler, "channelA", null, subscriptionId);
        List<String> batch1 = messages(20, 40);
        List<String> batch2 = messages(20, 40);

        send(topic, batch1, 0, 0, publisher);
        Util.sleepQuietly(5000);
        // Unsubscribe after the first batch.
        Assert.assertTrue(subscriber.unsubscribe(subscriptionId.get()));
        send(topic, batch2, 0, 0, publisher);

        Util.sleepQuietly(5000);

        // Ensure we only get 20 messages, even though we sent 40.
        List<NSQMessage> consumerMessages = handler.drainMessages(20);
        Assert.assertEquals(20, consumerMessages.size());
    }

    @Test
    @Deprecated
    public void unsubscribingSubscribersByTopicAndChannel() {
        TestMessageHandler handler = new TestMessageHandler();
        Subscriber subscriber = startSubscriber(handler, "channelA", null, null);
        List<String> batch1 = messages(20, 40);
        List<String> batch2 = messages(20, 40);

        send(topic, batch1, 0, 0, publisher);
        Util.sleepQuietly(5000);
        // Unsubscribe after the first batch.
        Assert.assertTrue(subscriber.unsubscribe(topic, "channelA"));
        send(topic, batch2, 0, 0, publisher);

        Util.sleepQuietly(5000);

        // Ensure we only get 20 messages, even though we sent 40.
        List<NSQMessage> consumerMessages = handler.drainMessages(20);
        awaitNoInFlightMessages(subscriber);
        Assert.assertEquals(20, consumerMessages.size());
    }

    // A message handler that deliberately processes messages "forever", to simulate
    // in-flight message handling.
    private static class HangingMessageHandler implements MessageHandler {
        private final long delayMs;

        public HangingMessageHandler() {
            // Hang forever.
            this(Long.MAX_VALUE);
        }

        public HangingMessageHandler(final long delayMs) {
            this.delayMs = delayMs;
        }

        @Override
        public void accept(Message msg) {
            try {
                Thread.sleep(delayMs);
                msg.finish();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    public void unsubscribeWithMessagesInFlight() {
        // Deliberately use a message handler that hangs forever, causing messages
        // to stay in flight.
        HangingMessageHandler handler = new HangingMessageHandler();
        AtomicReference<SubscriptionId> subscriptionId = new AtomicReference<>();
        Subscriber subscriber = startSubscriber(handler, "channelA", null, subscriptionId);
        List<String> batch1 = messages(20, 40);

        send(topic, batch1, 0, 0, publisher);
        Util.sleepQuietly(5000);
        final Subscription subscription = subscriber.unsubscribeSubscription(subscriptionId.get());
        // Since messages are in flight, we won't close the subscription immediately
        Assert.assertEquals(1, subscription.getConnectionCount());

        // Wait for the connection count to drop to 0
        for (int i = 0; i < 30; i++) {
            final int count = subscription.getConnectionCount();
            if (count > 0) {
                logger.info("Connection count still at:{}, iteration:{}, waiting...", count, i);
                Util.sleepQuietly(1000);
            } else {
                return;
            }
        }
        Assert.fail("Never got to connection count 0, failing");
    }

    @Test
    public void drainInFlight() {
        // Deliberately use a message handler that hangs for awhile, causing messages
        // to stay in flight.
        HangingMessageHandler handler = new HangingMessageHandler(5000);
        AtomicReference<SubscriptionId> subscriptionId = new AtomicReference<>();
        Subscriber subscriber = startSubscriber(handler, "channelA", null, subscriptionId);
        List<String> batch1 = messages(20, 40);

        send(topic, batch1, 0, 0, publisher);
        Util.sleepQuietly(5000);
        subscriber.drainInFlight();

        // Wait for in-flight count to drop to 0
        for (int i = 0; i < 30; i++) {
            final int count = subscriber.getCurrentInFlightCount();
            if (count > 0) {
                logger.info("In-flight count still at:{}, iteration:{}, waiting...", count, i);
                Util.sleepQuietly(1000);
            } else {
                return;
            }
        }
        Assert.fail("Never got to in-flight count 0, failing");
    }

    // A client is not allowed to send a CLS command until a SUB command
    // has been successfully received by the server. Ensure that we can successfully
    // unsubscribe while we still can't locate the correct nsqd from the lookup nodes.
    @Test
    public void unsubscribeBeforeSubscriptionIsEstablished() {
        TestMessageHandler handler = new TestMessageHandler();
        AtomicReference<SubscriptionId> subscriptionId = new AtomicReference<>();
        Subscriber subscriber = startSubscriber(handler, "channelA", null, subscriptionId);
        Assert.assertTrue(subscriber.unsubscribe(subscriptionId.get()));
    }

    @Test
    public void verySlowConsumer_allMessagesReceivedByResponsiveConsumer() {
        TestMessageHandler handler = new TestMessageHandler();
        NoAckReceiver delayHandler = new NoAckReceiver(8000);
        final Subscriber subscriber1 = startSubscriber(handler, "channelA", null, null);
        final Subscriber subscriber2 = startSubscriber(delayHandler, "channelA", null, null);
        List<String> messages = messages(40, 40);

        send(topic, messages, 1, 100, publisher);

        List<NSQMessage> firstConsumerMessages = handler.drainMessagesOrTimeOut(40, 15000);
        List<NSQMessage> delayedMessages = delayHandler.drainMessages(40);
        awaitNoInFlightMessages(subscriber1);
        Assert.assertFalse("Expect the consumer that doesn't ack to have received some messages", delayedMessages.isEmpty());

        validateReceivedAllMessages(messages, firstConsumerMessages, false);
    }

    @Override
    public void teardown() throws InterruptedException {
        if (publisher != null) {
            publisher.stop();
        }
        for (Subscriber subscriber : subscribers) {
            subscriber.stop();
        }
        subscribers.clear();
        super.teardown();
    }

    private Subscriber startSubscriber(MessageHandler handler, String channel, FailedMessageHandler failedMessageHandler, AtomicReference<SubscriptionId> subscriptionIdRef) {
        Subscriber subscriber = new Subscriber(client, 1, 10, cluster.getLookupNode().getHttpHostAndPort().toString());
        subscriber.setDefaultMaxInFlight(1);
        subscriber.setMaxAttempts(5);
        if (failedMessageHandler != null) {
            subscriber.setFailedMessageHandler(failedMessageHandler);
        }
        final SubscriptionId subscriptionId = subscriber.subscribe(topic, channel, handler);
        if (subscriptionIdRef != null) {
            subscriptionIdRef.set(subscriptionId);
        }
        this.subscribers.add(subscriber);
        return subscriber;
    }

    private void awaitNoInFlightMessages(final Subscriber subscriber) {
        while (subscriber.getCurrentInFlightCount() > 0) { }
    }
}
