package com.sproutsocial.nsq;

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

//clases that end in IT are integration tests run by maven failsafe plugin
public class SubscribeIT extends SubscribeBase {

    @Test
    public void testSub() throws IOException {
        List<String> msgs = messages(300, 800);

        //test with #ephemeral to test encoding. ephemeral topics enqueue until the first channel is created
        String topic = "subtest#ephemeral";
        post("localhost:4151", topic, "pub", msgs.get(0));

        Util.sleepQuietly(1000);

        Subscriber subscriber = new Subscriber(10, "127.0.0.1");
        subscriber.subscribe(topic, "chan", handler);


        postMessages("localhost:4151", topic, msgs.subList(1, msgs.size()));

        Util.sleepQuietly(2000);
        subscriber.stop();

        Collections.sort(received);
        //debugFail(received, msgs);
        assertEquals(msgs, received);
    }

    @Test
    public void testLookup() throws IOException {
        Subscriber subscriber = new Subscriber(10, "127.0.0.1");
        subscriber.subscribe("first", "ch", handler);
        subscriber.subscribe("after", "ch", handler);

        post("localhost:4151", "first", "pub", "msg one");
        Util.sleepQuietly(12000);

        assertEquals(received, Arrays.asList("msg one"));

        post("localhost:4151", "after", "pub", "msg two");
        Util.sleepQuietly(12000);

        assertEquals(received, Arrays.asList("msg one", "msg two"));
        subscriber.stop();
    }

    @Test
    public void testReconnect() throws Exception {
        String topic = "t3";

        post("localhost:4151", topic, "pub", "before disconnect");
        Util.sleepQuietly(2000);

        Subscriber subscriber = new Subscriber(4, "127.0.0.1");
        subscriber.setDefaultMaxInFlight(2);
        subscriber.subscribe(topic, "ch", handler);
        Util.sleepQuietly(2000);

        assertEquals(received, Arrays.asList("before disconnect"));
        exec("src/test/script/restart_nsqd.sh");

        post("localhost:4151", topic, "pub", "after reconnect");
        Util.sleepQuietly(8000);

        assertEquals(received, Arrays.asList("before disconnect", "after reconnect"));
        subscriber.stop();
    }

}
