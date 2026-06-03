package com.sproutsocial.nsq;

import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Verifies that a thread interrupt does not cause Publisher to mark healthy hosts as failed.
 *
 * Regression for the 2026-05-08 incident: markFailure() was called unconditionally in the
 * catch block, so a single interrupt cascaded across all connected hosts within milliseconds.
 */
public class PublisherInterruptTest {

    @After
    public void clearInterruptFlag() {
        Thread.interrupted();
    }

    // ---- Test doubles ----

    private static class SingleInstanceStrategy implements BalanceStrategy {
        final NsqdInstance instance;

        SingleInstanceStrategy(NsqdInstance instance) {
            this.instance = instance;
        }

        @Override public NsqdInstance getNsqdInstance() { return instance; }
        @Override public void connectionClosed(PubConnection c) {}
        @Override public int getFailoverDurationSecs() { return 300; }
        @Override public void setFailoverDurationSecs(int s) {}
    }

    /** NsqdInstance subclass that counts markFailure() calls and returns a fixed PubConnection. */
    static class SpyNsqdInstance extends NsqdInstance {
        int markFailureCount = 0;
        private final PubConnection fakeCon;

        SpyNsqdInstance(Client client, Publisher parent, PubConnection fakeCon) {
            super(client, "localhost:4150", parent, 300);
            this.fakeCon = fakeCon;
        }

        @Override
        public synchronized void markFailure() {
            markFailureCount++;
        }

        @Override
        public PubConnection getCon() {
            return fakeCon;
        }
    }

    /** PubConnection subclass whose publish methods always throw NSQInterruptedException. */
    static class InterruptingPubConnection extends PubConnection {
        InterruptingPubConnection(Client client, Publisher publisher) {
            super(client, HostAndPort.fromString("localhost:4150"), publisher);
        }

        @Override
        public synchronized void publish(String topic, byte[] data) throws IOException {
            throw new NSQInterruptedException("read interrupted");
        }

        @Override
        public synchronized void publish(String topic, List<byte[]> dataList) throws IOException {
            throw new NSQInterruptedException("read interrupted");
        }
    }

    /** PubConnection subclass whose publish(byte[]) throws a plain NSQException (real failure). */
    static class FailingPubConnection extends PubConnection {
        FailingPubConnection(Client client, Publisher publisher) {
            super(client, HostAndPort.fromString("localhost:4150"), publisher);
        }

        @Override
        public synchronized void publish(String topic, byte[] data) throws IOException {
            throw new NSQException("bad response:timeout");
        }
    }

    /** PubConnection subclass whose publish methods are no-ops (success). */
    static class SucceedingPubConnection extends PubConnection {
        SucceedingPubConnection(Client client, Publisher publisher) {
            super(client, HostAndPort.fromString("localhost:4151"), publisher);
        }

        @Override
        public synchronized void publish(String topic, byte[] data) throws IOException {}

        @Override
        public synchronized void publish(String topic, List<byte[]> dataList) throws IOException {}
    }

    // ---- Tests ----

    @Test
    public void publish_byte_propagatesInterruptWithoutMarkingFailure() {
        SpyNsqdInstance[] spyRef = new SpyNsqdInstance[1];
        Publisher publisher = new Publisher(Client.getDefaultClient(), (client, pub) -> {
            SpyNsqdInstance spy = new SpyNsqdInstance(client, pub, new InterruptingPubConnection(client, pub));
            spyRef[0] = spy;
            return new SingleInstanceStrategy(spy);
        });

        try {
            publisher.publish("topic", new byte[]{0x01});
            fail("NSQInterruptedException must propagate to caller");
        } catch (NSQInterruptedException e) {
            // expected
        }

        assertEquals("markFailure must NOT be called on thread interrupt", 0, spyRef[0].markFailureCount);
    }

    @Test
    public void publish_list_propagatesInterruptWithoutMarkingFailure() {
        SpyNsqdInstance[] spyRef = new SpyNsqdInstance[1];
        Publisher publisher = new Publisher(Client.getDefaultClient(), (client, pub) -> {
            SpyNsqdInstance spy = new SpyNsqdInstance(client, pub, new InterruptingPubConnection(client, pub));
            spyRef[0] = spy;
            return new SingleInstanceStrategy(spy);
        });

        try {
            publisher.publish("topic", Arrays.asList(new byte[]{0x01}, new byte[]{0x02}));
            fail("NSQInterruptedException must propagate to caller");
        } catch (NSQInterruptedException e) {
            // expected
        }

        assertEquals("markFailure must NOT be called on thread interrupt", 0, spyRef[0].markFailureCount);
    }

    @Test
    public void normalException_stillCallsMarkFailure() {
        // First call fails with a real error; retry to second instance succeeds.
        SpyNsqdInstance[] spyRef = new SpyNsqdInstance[2];

        Publisher publisher = new Publisher(Client.getDefaultClient(), (client, pub) -> {
            spyRef[0] = new SpyNsqdInstance(client, pub, new FailingPubConnection(client, pub));
            spyRef[1] = new SpyNsqdInstance(client, pub, new SucceedingPubConnection(client, pub));
            int[] calls = {0};
            return new BalanceStrategy() {
                @Override public NsqdInstance getNsqdInstance() { return spyRef[calls[0]++ == 0 ? 0 : 1]; }
                @Override public void connectionClosed(PubConnection c) {}
                @Override public int getFailoverDurationSecs() { return 300; }
                @Override public void setFailoverDurationSecs(int s) {}
            };
        });

        publisher.publish("topic", new byte[]{0x01});

        assertEquals("failing host must be marked failed", 1, spyRef[0].markFailureCount);
        assertEquals("healthy retry host must NOT be marked failed", 0, spyRef[1].markFailureCount);
    }
}
