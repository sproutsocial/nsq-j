package com.sproutsocial.nsq;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

//clases that end in IT are integration tests run by maven failsafe plugin
public class SubscribeTimeoutIT extends SubscribeBase {

    @BeforeClass
    public static void beforeTests() throws Exception {
        System.out.println("initial random seed " + seed);

        // 5s so the consumer has time to grab the message before it times out
        exec("src/test/script/start_tests.sh -msg-timeout=0m5s");
    }

    @Test
    public void testMessageTimeout() throws IOException {
        List<String> msgs = messages(1, 800);

        //test with #ephemeral to test encoding. ephemeral topics enqueue until the first channel is created
        String topic = "subtest#ephemeral";
        post("localhost:4151", topic, "pub", msgs.get(0));

        Util.sleepQuietly(1000);

        ObservedConnectionClient client = new ObservedConnectionClient();
        try {
            Subscriber subscriber = new Subscriber(client, 10, 5, "127.0.0.1");
            subscriber.subscribe(topic, "chan", 1, new MessageHandler() {
                @Override
                public void accept(Message message) {
                    try {
                        // sleep long enough for nsqd to timeout message
                        Thread.sleep(8000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } finally {
                        message.finish();
                    }
                }
            });

            Util.sleepQuietly(15000);

            // verify the connection wasn't closed on timeout, and then re-connected
            assertFalse(client.connectionClosedCalled());

            subscriber.stop();
        }
        finally {
            assertTrue(client.stop());
        }
    }
}
