package com.sproutsocial.nsq;

import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for NsqdInstance state transitions, focusing on the clearAllConnections() fix.
 *
 * Regression for the 2026-05-08 incident: clearAllConnections() reset state to NOT_CONNECTED,
 * discarding failoverStart and bypassing the backoff on every subsequent publish attempt.
 */
public class NsqdInstanceStateTest {

    /** PubConnection that tracks close() calls without opening a real socket. */
    static class TrackingPubConnection extends PubConnection {
        boolean closeCalled = false;

        TrackingPubConnection(Client client, Publisher publisher) {
            super(client, HostAndPort.fromString("localhost:4150"), publisher);
        }

        @Override public synchronized void publish(String topic, byte[] data) throws IOException {}
        @Override public synchronized void publish(String topic, List<byte[]> dataList) throws IOException {}

        @Override
        public void close() {
            closeCalled = true;
            // skip super — no real socket to close
        }
    }

    private static Publisher stubPublisher() {
        return new Publisher(Client.getDefaultClient(), (c, p) -> new BalanceStrategy() {
            @Override public NsqdInstance getNsqdInstance() { throw new NSQException("not used"); }
            @Override public void connectionClosed(PubConnection con) {}
            @Override public int getFailoverDurationSecs() { return 300; }
            @Override public void setFailoverDurationSecs(int s) {}
        });
    }

    private static NsqdInstance freshInstance() {
        return new NsqdInstance(Client.getDefaultClient(), "localhost:4150", stubPublisher(), 300);
    }

    @Test
    public void markFailureIfNotAlready_setsFailoverStartWhenNotConnected() {
        NsqdInstance instance = freshInstance();
        long before = Util.clock();
        instance.markFailureIfNotAlready();
        assertTrue("failoverStart must be set after transitioning NOT_CONNECTED → FAILED",
                instance.failoverStart >= before);
    }

    @Test
    public void markFailureIfNotAlready_doesNotResetTimerWhenAlreadyFailed() {
        NsqdInstance instance = freshInstance();
        instance.markFailure();
        long originalStart = instance.failoverStart;

        Util.sleepQuietly(5);

        instance.markFailureIfNotAlready();

        assertEquals("failoverStart must NOT be reset when host is already FAILED — "
                + "resetting would prevent recovery indefinitely",
                originalStart, instance.failoverStart);
    }

    @Test
    public void clearConnection_closesSocketBeforeNullingReference() {
        Publisher publisher = stubPublisher();
        NsqdInstance instance = new NsqdInstance(
                Client.getDefaultClient(), "localhost:4150", publisher, 300);
        TrackingPubConnection con = new TrackingPubConnection(Client.getDefaultClient(), publisher);
        instance.con = con;

        instance.clearConnection();

        assertTrue("clearConnection() must close the socket to prevent TCP fd leak",
                con.closeCalled);
    }
}
