package com.sproutsocial.nsq;

import org.junit.Ignore;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

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
    public void clearsConnectionsIfAllNodesAreMarkedDead() {
        // Force publishing to fail by disconnecting all the nodes network
        // Perhaps to simulate a bad network flake.
        cluster.getNsqdNodes().forEach(cluster::disconnectNetworkFor);

        boolean exceptionTriggered = false;
        try {
            publisher.publish(topic, new byte[]{0x0});
        } catch (NSQException e) {
            exceptionTriggered = true;
        }

        assertTrue(exceptionTriggered);

        // As soon as the network flakiness subsides, we should be in a state
        // where publishing succeeds again immediately.
        cluster.getNsqdNodes().forEach(cluster::reconnectNetworkFor);

        sendAndVerifyMessagesFromPrimary(publisher, handler);
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
