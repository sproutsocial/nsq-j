package com.sproutsocial.nsq;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.assertEquals;

//run one test: mvn "-Dit.test=PublishIT#testSnappy" verify

public class RoundRobbinIT extends SubscribeBase {

    public static final String PUBTEST = "pubtest";
    public static final String MPUBTEST = "mpubtest";
    public static final int MAX_MSG_LEN = 20;
    public static final int MESSAGES_TO_SEND = 50;
    private boolean usePublishSoon = false;

    private void send(Publisher publisher, String topic, List<String> msgs, float delayChance, int maxDelay) {
        int count = 0;
        for (String msg : msgs) {
            if (random.nextFloat() < delayChance) {
                Util.sleepQuietly(random.nextInt(maxDelay));
            }
            if (usePublishSoon) {
                publisher.publishBuffered(topic, msg.getBytes());
            } else {
                publisher.publish(topic, msg.getBytes());
            }
            if (++count % 10 == 0) {
                System.out.println("sent " + count + " msgs");
            }
        }
    }

    @Test
    public void testPubSoon() throws Exception {
        System.out.println("testPubSoon. random seed:" + seed);
        usePublishSoon = true;
        testPub(new Config(), MAX_MSG_LEN);
        usePublishSoon = false;
    }

    @Test
    public void testPub() throws Exception {
        System.out.println("testPub. random seed:" + seed);
        testPub(new Config(), MAX_MSG_LEN);
    }

    private void testPub(Config config, int maxMsgLen) throws Exception {

        Subscriber subscriber = new Subscriber(10, "127.0.0.1");
        subscriber.subscribe(PUBTEST, "tail" + this.usePublishSoon + "#ephemeral", handler);

        Util.sleepQuietly(1000);

        Publisher publisher = new Publisher("localhost,test_nsqd");
        publisher.setConfig(config);
        List<String> msgs = messages(MESSAGES_TO_SEND, maxMsgLen);
        if (usePublishSoon) {
            send(publisher, PUBTEST, msgs, 0.045f, 1200); //nice mix of delay/size at maxDelay=300 maxSize=16k
        } else {
            send(publisher, PUBTEST, msgs, 0.1f, 1000);
        }

        Util.sleepQuietly(1000);

        // Since we are publishing round robin very rapidly, there is a good chance that messages will be delivered out of order.
        // That is ok if you are using round robin publishing.
        Collections.sort(received, new Comparator<String>() {
            @Override
            public int compare(String s, String anotherString) {
                return s.compareTo(anotherString);
            }
        });
        assertEquals(msgs, received);
    }

    @Test
    public void testMultiPub() throws Exception {
        System.out.println("testMultiPub. random seed:" + seed);
        testMultiPub(new Config());
    }

    private void testMultiPub(Config config) throws Exception {
        Subscriber subscriber = new Subscriber(10, "127.0.0.1");
        subscriber.subscribe(MPUBTEST, "tailMulti#ephemeral", handler);

        Util.sleepQuietly(1000);

        Publisher publisher = new Publisher("localhost,test_nsqd");
        publisher.setConfig(config);

        List<String> sent = new ArrayList<String>();

        int count = 0;
        for (int i = 0; i < 4; i++) {
            List<String> msgs = messages(10, MAX_MSG_LEN);
            List<byte[]> msgData = new ArrayList<byte[]>(msgs.size());
            for (String msg : msgs) {
                msgData.add(msg.getBytes());
            }
            publisher.publish(MPUBTEST, msgData);
            count += msgs.size();
            sent.addAll(msgs);
            System.out.println("mpub " + count);
            Util.sleepQuietly(random.nextInt(500));
        }

        Util.sleepQuietly(1000);

        //With round robbin, messages may arrive out of order.
        Collections.sort(received, new Comparator<String>() {
            @Override
            public int compare(String s, String anotherString) {
                return s.compareTo(anotherString);
            }
        });
        Collections.sort(sent, new Comparator<String>() {
            @Override
            public int compare(String s, String anotherString) {
                return s.compareTo(anotherString);
            }
        });
        assertEquals(sent, received);
    }
}
