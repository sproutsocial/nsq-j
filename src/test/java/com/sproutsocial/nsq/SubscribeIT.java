package com.sproutsocial.nsq;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

//clases that end in IT are integration tests run by maven failsafe plugin
public class SubscribeIT extends TestBase {

    private List<String> received = Lists.newArrayList();
    private MessageHandler handler;

    public SubscribeIT() {
        handler = new MessageHandler() {
            public void accept(Message msg) {
                handle(msg);
            }
        };
    }

    @BeforeClass
    public static void beforeTests() throws Exception {
        exec("src/test/script/start_tests.sh");
    }

    private void post(String host, String topic, String command,  String body) throws IOException {
        URL url = new URL(String.format("http://%s/%s?topic=%s", host, command, topic));
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("POST");
        con.setDoOutput(true);
        con.getOutputStream().write(body.getBytes());
        con.getOutputStream().close();
        System.out.println("post resp:" + con.getResponseCode() + " " + con.getResponseMessage());
    }

    private void postMessages(String host, String topic, List<String> msgs) throws IOException {
        int i = 0;
        while (i < msgs.size()) {
            if (random.nextFloat() < 0.05) {
                post(host, topic, "pub", msgs.get(i));
                i++;
            }
            else {
                int count = 1 + random.nextInt(40);
                int end = Math.min(msgs.size(), i + count);
                String body = Joiner.on('\n').join(msgs.subList(i, end));
                post(host, topic, "mpub", body);
                i += count;
            }
            if (random.nextFloat() < 0.5) {
                Util.sleepQuietly(random.nextInt(1000));
            }
        }
    }

    private void handle(Message msg) {
        //System.out.println("rec:" + new String(msg.getData()).substring(0, Math.min(18, msg.getData().length)));
        received.add(new String(msg.getData()));
        msg.finish();
    }

    @Test
    public void testSub() throws IOException {
        received.clear();

        List<String> msgs = messages(300, 800);

        String topic = "subtest";
        post("localhost:4151", topic, "pub", msgs.get(0));

        Util.sleepQuietly(1000);

        Subscriber subscriber = new Subscriber(10, "127.0.0.1");
        subscriber.subscribe(topic, "chan", handler);


        postMessages("localhost:4151", topic, msgs.subList(1, msgs.size()));

        Util.sleepQuietly(2000);
        subscriber.stop();

        Collections.sort(received);
        assertEquals(msgs, received);
    }

    @Test
    public void testLookup() throws IOException {
        received.clear();

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
        received.clear();
        String topic = "t3";

        post("localhost:4151", topic, "pub", "before disconnect");
        Util.sleepQuietly(2000);

        Subscriber subscriber = new Subscriber(10, "127.0.0.1");
        subscriber.setMaxInFlightPerSubscription(2);
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
