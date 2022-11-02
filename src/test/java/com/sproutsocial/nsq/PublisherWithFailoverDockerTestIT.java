package com.sproutsocial.nsq;

import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class PublisherWithFailoverDockerTestIT extends BaseDockerTestIT {
    private Subscriber subscriber;
    private Publisher publisher;
    private TestMessageHandler handler;

    @Override
    public void setup() {
        super.setup();
        Util.sleepQuietly(500);
        handler = new TestMessageHandler();
        subscriber = new Subscriber(client, 1, 5, cluster.getLookupNode().getHttpHostAndPort().toString());
        subscriber.subscribe(topic, "tail" + System.currentTimeMillis(), handler);
        publisher = backupPublisher();
    }

    @Override
    public void teardown() throws InterruptedException {
        subscriber.stop();
        if (publisher != null) {
            publisher.stop();
        }
        super.teardown();
    }


    @Test
    public void testBasicRoundTrip_WithBackup_noFailure() {
        sendAndVerifyMessagesFromPrimary(publisher, handler);
    }

    @Test
    public void withBackup_failureAfterSomeMessagesArePublished() {
        sendAndVerifyMessagesFromPrimary(publisher, handler);

        cluster.disconnectNetworkFor(cluster.getNsqdNodes().get(0));

        sendAndVerifyMessagesFromBackup(publisher, handler);
    }

    @Test
    public void withBackup_failoverAndFailBackProactivly() {
        sendAndVerifyMessagesFromPrimary(publisher, handler);

        cluster.disconnectNetworkFor(cluster.getNsqdNodes().get(0));

        sendAndVerifyMessagesFromBackup(publisher, handler);

        cluster.reconnectNetworkFor(cluster.getNsqdNodes().get(0));
        //Warning, this should not take so long.  But it does seem to work reliably.
        Util.sleepQuietly(TimeUnit.SECONDS.toMillis(15));

        sendAndVerifyMessagesFromPrimary(publisher, handler);
    }

    @Test
    public void failedBulkPublishesPublishToFailoverSequentially() {
        final int maxBodySize = 5242880; // This is the default nsqd max body size for any nsqd TCP message
        final int count = 30;
        final int bytesPerMessage = (maxBodySize + 10) / (count - 1);
        final List<String> messages = messages(count, bytesPerMessage);
        final List<byte[]> batch = messages
            .stream()
            .map(String::getBytes)
            .collect(Collectors.toList());
        publisher.publish(topic, batch);
        List<NSQMessage> receivedMessages = handler.drainMessagesOrTimeOut(messages.size());
        validateReceivedAllMessages(messages, receivedMessages, true);
        validateFromParticularNsqd(receivedMessages, 1);
    }

    @Test
    @Ignore("This one actually fails given the current behavior of the system")
    public void withBackup_failoverAndFailbackRightAwayIfBackupGoesDown() {
        sendAndVerifyMessagesFromPrimary(publisher, handler);

        cluster.disconnectNetworkFor(cluster.getNsqdNodes().get(0));

        sendAndVerifyMessagesFromBackup(publisher, handler);

        cluster.reconnectNetworkFor(cluster.getNsqdNodes().get(0));
        cluster.disconnectNetworkFor(cluster.getNsqdNodes().get(1));

        sendAndVerifyMessagesFromPrimary(publisher, handler);
    }
}
