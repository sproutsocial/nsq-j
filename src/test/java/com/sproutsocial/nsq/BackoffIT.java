package com.sproutsocial.nsq;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BackoffIT extends SubscribeBase {

    private final List<Integer> handleDelay = new ArrayList<Integer>();
    private long lastHandleTime;
    private Queue<Boolean> handleSucceedQueue = new ArrayDeque<Boolean>();

    @Override
    @Before
    public void beforeTest() {
        super.beforeTest();
        handleDelay.clear();
    }

    @Override
    protected synchronized void handle(Message msg) {
        long now = Util.clock();
        long delay = now - lastHandleTime;
        handleDelay.add((int) delay);
        lastHandleTime = now;

        System.out.println("delay:" + delay + " " + new String(msg.getData()).substring(0, 17));
        if (handleSucceedQueue.isEmpty() || handleSucceedQueue.remove()) {
            received.add(new String(msg.getData()));
        }
        else {
            throw new RuntimeException("test backoff");
        }
    }

    private static void assertApproxEquals(List<Integer> l1, List<Integer> l2, int eps) {
        assertEquals(l1.size(), l2.size());
        for (int i = 0; i < l1.size(); i++) {
            assertTrue("index:" + i + "  " + l1.get(i) + "=?" + l2.get(i), Math.abs(l1.get(i) - l2.get(i)) < eps);
        }
    }

    @Test
    public void testBackoff() throws IOException {
        List<String> msgs = messages(10, 500);

        post("localhost:4151", "backoff", "pub", msgs.get(0));
        post("localhost:4151", "backoff", "pub", msgs.get(1));
        post("localhost:4151", "backoff", "pub", msgs.get(2));
        Util.sleepQuietly(1000);

        handleSucceedQueue.addAll(Arrays.asList(true, false, false, false, true, true, true, true));
        lastHandleTime = Util.clock();
        Subscriber subscriber = new Subscriber(90, "127.0.0.1");
        subscriber.setDefaultMaxInFlight(1);
        subscriber.subscribe("backoff", "chan", new BackoffHandler(handler));

        Util.sleepQuietly(1000);
        postMessages("localhost:4151", "backoff", msgs.subList(3, msgs.size()));

        Util.sleepQuietly(10000);

        System.out.println("delays:" + handleDelay);
        List<Integer> expectedDelay = Arrays.asList(0, 0, 1000, 2000, 4000, 2000, 1000, 0, 0, 0);
        assertApproxEquals(handleDelay.subList(0, expectedDelay.size()), expectedDelay, 500);

        Collections.sort(received);
        assertEquals(msgs, received);
    }
}
