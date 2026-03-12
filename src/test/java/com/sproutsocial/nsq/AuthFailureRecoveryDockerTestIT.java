package com.sproutsocial.nsq;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Integration test for immediate reconnection behavior.
 *
 * These tests verify that the immediateCheckConnections() method (which is called
 * when auth failures occur) works correctly to trigger immediate reconnection
 * rather than waiting up to 60 seconds for the periodic checkConnections() call.
 *
 * Note: Actual E_AUTH_FAILED/E_UNAUTHORIZED error handling is tested in
 * ConnectionAuthFailureTest, which verifies those errors throw AuthFailedException
 * and trigger the handleAuthFailure() -> immediateCheckConnections() code path.
 * These integration tests verify the reconnection mechanism itself works correctly.
 */
public class AuthFailureRecoveryDockerTestIT extends BaseDockerTestIT {
    private static final Logger logger = LoggerFactory.getLogger(AuthFailureRecoveryDockerTestIT.class);
    private Publisher publisher;
    private Subscriber subscriber;

    @Override
    public void setup() {
        super.setup();
        publisher = this.backupPublisher();
    }

    /**
     * Tests that the subscriber can successfully establish connections and receive messages.
     * This baseline test ensures the immediate reconnection mechanism doesn't break
     * normal operation.
     */
    @Test
    public void testNormalSubscriberOperation() {
        TestMessageHandler handler = new TestMessageHandler();
        subscriber = new Subscriber(client, 1, 10, cluster.getLookupNode().getHttpHostAndPort().toString());
        subscriber.setDefaultMaxInFlight(10);
        subscriber.subscribe(topic, "channel", handler);

        // Send messages and verify they're received
        List<String> messages = messages(20, 40);
        send(topic, messages, 0, 0, publisher);

        // Verify all messages received
        List<NSQMessage> receivedMessages = handler.drainMessagesOrTimeOut(20);
        validateReceivedAllMessages(messages, receivedMessages, false);
    }

    /**
     * Tests that subscriber can recover from connection drops.
     * This simulates a scenario similar to auth failure where connection is lost
     * and needs to be re-established.
     */
    @Test
    public void testSubscriberReconnectionAfterNetworkDisruption() {
        TestMessageHandler handler = new TestMessageHandler();
        subscriber = new Subscriber(client, 1, 10, cluster.getLookupNode().getHttpHostAndPort().toString());
        subscriber.setDefaultMaxInFlight(10);
        subscriber.subscribe(topic, "channel", handler);

        // Send first batch of messages
        List<String> batch1 = messages(10, 40);
        send(topic, batch1, 0, 0, publisher);
        List<NSQMessage> received1 = handler.drainMessagesOrTimeOut(10);
        validateReceivedAllMessages(batch1, received1, false);

        // Simulate connection issue by disconnecting and reconnecting network
        logger.info("Disconnecting network for nsqd node to simulate connection drop");
        cluster.disconnectNetworkFor(cluster.getNsqdNodes().get(0));
        Util.sleepQuietly(2000);

        cluster.reconnectNetworkFor(cluster.getNsqdNodes().get(0));
        Util.sleepQuietly(2000);

        // Send second batch and verify recovery
        List<String> batch2 = messages(10, 40);
        send(topic, batch2, 0, 0, publisher);
        List<NSQMessage> received2 = handler.drainMessagesOrTimeOut(10, 20000);
        validateReceivedAllMessages(batch2, received2, false);

        logger.info("Successfully received messages after connection recovery");
    }

    /**
     * Tests that the immediateCheckConnections method is accessible and functional.
     * This verifies the immediate reconnection path exists.
     */
    @Test
    public void testImmediateCheckConnectionsMethod() {
        TestMessageHandler handler = new TestMessageHandler();
        subscriber = new Subscriber(client, 1, 10, cluster.getLookupNode().getHttpHostAndPort().toString());
        subscriber.setDefaultMaxInFlight(10);
        subscriber.subscribe(topic, "channel", handler);

        // Send initial messages to establish connection
        List<String> messages = messages(5, 40);
        send(topic, messages, 0, 0, publisher);
        handler.drainMessagesOrTimeOut(5);

        // Call immediateCheckConnections (this is what gets called on auth failure)
        subscriber.immediateCheckConnections(topic);

        // Verify subscriber still works after immediate check
        List<String> moreMessages = messages(5, 40);
        send(topic, moreMessages, 0, 0, publisher);
        List<NSQMessage> received = handler.drainMessagesOrTimeOut(5);
        validateReceivedAllMessages(moreMessages, received, false);

        logger.info("immediateCheckConnections method works correctly");
    }

    /**
     * Tests that multiple rapid connection checks don't cause issues.
     * This simulates what might happen if multiple connections experience auth failures.
     */
    @Test
    public void testMultipleRapidConnectionChecks() {
        TestMessageHandler handler = new TestMessageHandler();
        subscriber = new Subscriber(client, 1, 10, cluster.getLookupNode().getHttpHostAndPort().toString());
        subscriber.setDefaultMaxInFlight(10);
        subscriber.subscribe(topic, "channel", handler);

        // Send initial messages to establish connections
        List<String> messages = messages(10, 40);
        send(topic, messages, 0, 0, publisher);
        handler.drainMessagesOrTimeOut(10);

        // Verify connections are established
        Assert.assertTrue("Should have connections", subscriber.getConnectionCount() > 0);

        // Call immediateCheckConnections multiple times rapidly
        // (simulating multiple auth failures in quick succession)
        for (int i = 0; i < 5; i++) {
            subscriber.immediateCheckConnections(topic);
            Util.sleepQuietly(100);
        }

        // Verify subscriber still works correctly
        List<String> moreMessages = messages(10, 40);
        send(topic, moreMessages, 0, 0, publisher);
        List<NSQMessage> received = handler.drainMessagesOrTimeOut(10);
        validateReceivedAllMessages(moreMessages, received, false);

        logger.info("Multiple rapid connection checks handled correctly");
    }

    /**
     * Verifies that the subscriber's connection count updates correctly after
     * immediate connection checks (which would happen during auth failure recovery).
     */
    @Test
    public void testConnectionCountAfterImmediateReconnect() {
        TestMessageHandler handler = new TestMessageHandler();
        subscriber = new Subscriber(client, 1, 10, cluster.getLookupNode().getHttpHostAndPort().toString());
        subscriber.setDefaultMaxInFlight(10);
        subscriber.subscribe(topic, "channel", handler);

        // Send messages to create topic and establish initial connections
        List<String> initialMessages = messages(5, 40);
        send(topic, initialMessages, 0, 0, publisher);
        handler.drainMessagesOrTimeOut(5);

        int initialConnectionCount = subscriber.getConnectionCount();
        Assert.assertTrue("Should have at least one connection", initialConnectionCount > 0);
        logger.info("Initial connection count: {}", initialConnectionCount);

        // Trigger immediate check (simulating auth failure recovery)
        subscriber.immediateCheckConnections(topic);
        Util.sleepQuietly(2000);

        int afterCheckCount = subscriber.getConnectionCount();
        Assert.assertTrue("Should still have connections after immediate check", afterCheckCount > 0);
        logger.info("Connection count after immediate check: {}", afterCheckCount);

        // Verify messages can still be processed
        List<String> messages = messages(10, 40);
        send(topic, messages, 0, 0, publisher);
        List<NSQMessage> received = handler.drainMessagesOrTimeOut(10);
        Assert.assertEquals("Should receive all messages after reconnect", 10, received.size());
    }

    /**
     * Tests that calling immediateCheckConnections with bad/non-existent hosts doesn't hang.
     * This simulates what would happen with genuinely bad credentials where
     * connection attempts fail but should complete quickly without infinite loops.
     *
     * Key behavior being tested:
     * - immediateCheckConnections() completes quickly even if connections fail
     * - No infinite loops - test completes in reasonable time
     * - Failed connections are not added to connectionMap
     */
    @Test
    public void testBadConnectionsDoNotCauseInfiniteLoop() {
        logger.info("Testing that failed connections don't cause infinite retry loops");

        // Create subscriber pointing to non-existent lookup server
        // This simulates a scenario where auth credentials are bad
        subscriber = new Subscriber(client, 300, 10, "127.0.0.1:54321");  // Unreachable port
        subscriber.setDefaultMaxInFlight(10);

        TestMessageHandler handler = new TestMessageHandler();
        subscriber.subscribe(topic, "channel", handler);

        // Verify no connections established (lookup fails)
        Util.sleepQuietly(1000);
        int initialConnectionCount = subscriber.getConnectionCount();
        Assert.assertEquals("Should have no connections with bad lookup", 0, initialConnectionCount);

        // Call immediateCheckConnections multiple times
        // If this caused an infinite loop, the test would hang here
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 5; i++) {
            subscriber.immediateCheckConnections(topic);
            Util.sleepQuietly(100);
        }
        long elapsedTime = System.currentTimeMillis() - startTime;

        // Verify still no connections (failed attempts not added)
        int finalConnectionCount = subscriber.getConnectionCount();
        Assert.assertEquals("Failed connections should not be in connectionMap", 0, finalConnectionCount);

        // Verify test completed quickly (no infinite loop)
        Assert.assertTrue("Should complete quickly without infinite loop", elapsedTime < 10000);

        logger.info("Successfully verified: Failed connections don't cause infinite retry loops");
        logger.info("- {} calls to immediateCheckConnections completed in {}ms", 5, elapsedTime);
        logger.info("- Connection count remained at 0 (failed connections not added)");
    }

    @Override
    public void teardown() throws InterruptedException {
        if (publisher != null) {
            publisher.stop();
        }
        if (subscriber != null) {
            subscriber.stop();
        }
        super.teardown();
    }
}
