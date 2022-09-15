package com.sproutsocial.nsq;

import org.junit.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BackoffDockerTestIT extends BaseDockerTestIT {
    private Subscriber subscriber;
    private Publisher publisher;

    private static class FailingHandler implements MessageHandler {
        private final Queue<Boolean> handleSucceedQueue = new ArrayDeque<>();
        private long lastHandleTime=Util.clock();
        private final List<Long> handleDelays = new ArrayList<>();

        public FailingHandler(boolean... success) {
            for (boolean b : success) {
                handleSucceedQueue.add(b);
            }
        }

        @Override
        public void accept(Message msg) {
            long now = Util.clock();
            long delay = now - lastHandleTime;
            handleDelays.add(delay);

            lastHandleTime = now;
            if(Boolean.FALSE.equals(handleSucceedQueue.poll())){
                throw new RuntimeException();
            }
        }
    }
    @Override
    public void setup() {
        super.setup();
    }

    @Test
    public void test(){
        FailingHandler failingHandler = new FailingHandler(true, false, false, false, true, true, true, true, true);
        subscriber = new Subscriber(client, 1, 5, cluster.getLookupNode().getHttpHostAndPort().toString());
        subscriber.setDefaultMaxInFlight(1);
        BackoffHandler backoffHandler = new BackoffHandler(failingHandler, 500, 4000);
        subscriber.subscribe(topic, "tail" + System.currentTimeMillis(), backoffHandler);
        publisher = primaryOnlyPublisher();

        List<String> messages = messages(8, 30);
        for (String message : messages) {
            publisher.publish(topic,message.getBytes());
        }

        List<Long> expectedDelay = Arrays.asList(0L, 500L, 1000L, 2000L, 1000L, 500L, 0L);
        Util.sleepQuietly(expectedDelay.stream().mapToInt(Math::toIntExact).sum()+2000);

        assertApproxEquals(expectedDelay, failingHandler.handleDelays.subList(1,expectedDelay.size()+1));
    }

    private static void assertApproxEquals(List<Long> l1, List<Long> l2) {
        System.out.println("Actual: "+l2);
        assertEquals(l1.size(), l2.size());
        for (int i = 0; i < l1.size(); i++) {
            assertTrue("index:" + i + "  " + l1.get(i) + "=?" + l2.get(i), Math.abs(l1.get(i) - l2.get(i)) < 250);
        }
    }

    @Override
    public void teardown() throws InterruptedException {
        subscriber.stop();

        if (publisher != null) {
            publisher.stop();
        }

        super.teardown();
    }
}
