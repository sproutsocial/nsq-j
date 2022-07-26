package com.sproutsocial.nsq;

import org.junit.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;


public class RoundTripIT extends SubscribeBase {

    public static final String PUBTEST = "pubtest";
    public static final int MAX_MSG_LEN = 20;
    public static final int MESSAGES_TO_SEND = 50;


    @Test
    public void testPub() {
        System.out.println("testPub. random seed:" + seed);
        testPub("localhost,test_nsqd", null);
    }

    @Test
    public void testSinglePub() {
        System.out.println("testSinglePub. random seed:" + seed);
        testPub("localhost", null);
    }

    @Test
    public void testFailoverPub() {
        System.out.println("testFailoverPub. random seed:" + seed);
        testPub("localhost", "test_nsqd");
    }

    private void testPub(String nsqd, String failoverNsqd) {

        Subscriber subscriber = new Subscriber(10, "127.0.0.1");
        subscriber.subscribe(PUBTEST, "tail" + UUID.randomUUID() + "#ephemeral", handler);

        Util.sleepQuietly(1000);

        Publisher publisher = new Publisher(nsqd, failoverNsqd);
        publisher.setConfig(new Config());
        List<String> msgs = messages(MESSAGES_TO_SEND, RoundTripIT.MAX_MSG_LEN);
        send(publisher, PUBTEST, msgs, 0.1f, 100);

        Util.sleepQuietly(1000);

        received.sort(String::compareTo);
        assertEquals(msgs, received);
    }

    private static void send(Publisher publisher, String topic, List<String> msgs, float delayChance, int maxDelay) {
        int count = 0;
        for (String msg : msgs) {
            if (random.nextFloat() < delayChance) {
                Util.sleepQuietly(random.nextInt(maxDelay));
            }
            publisher.publish(topic, msg.getBytes());
            if (++count % 10 == 0) {
                System.out.println("sent " + count + " msgs");
            }
        }
    }
}
