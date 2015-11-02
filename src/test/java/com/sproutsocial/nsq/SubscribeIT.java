package com.sproutsocial.nsq;

import com.google.common.collect.Lists;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

//clases that end in IT are integration tests run by maven failsafe plugin
public class SubscribeIT {

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
        System.out.println("starting tests");
        exec("src/test/script/start_tests.sh");
        System.out.println("tests started");
    }

    private static void exec(String command) throws IOException, InterruptedException {
        Process proc = Runtime.getRuntime().exec(command);
        proc.waitFor();
        readInput(proc.getInputStream());
        readInput(proc.getErrorStream());
        System.out.println(command + " exit:" + proc.exitValue());
    }

    private static void readInput(InputStream inputStream) throws IOException {
        BufferedReader in = new BufferedReader(new InputStreamReader(inputStream));
        String line = null;
        while ((line = in.readLine()) != null) {
            System.out.println(line);
        }
    }

    private void postMessage(String host, String topic, String msg) throws IOException {
        URL url = new URL(String.format("http://%s/put?topic=%s", host, topic));
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("POST");
        con.setDoOutput(true);
        con.getOutputStream().write(msg.getBytes());
        con.getOutputStream().close();
        System.out.println("post resp:" + con.getResponseCode() + " " + con.getResponseMessage());
    }

    private void handle(Message msg) {
        System.out.println("msg:" + new String(msg.getData()));
        received.add(new String(msg.getData()));
        msg.finish();
    }

    /*
    @Test
    public void testSub() throws IOException {
        received.clear();
        String topic = "t1";
        postMessage("localhost:4151", topic, "message 1.");
        postMessage("localhost:4151", topic, "new message 2.");
        Util.sleepQuietly(2000);

        Config config = new Config();
        Subscriber subscriber = new Subscriber(config, 10, "127.0.0.1");
        subscriber.subscribe(topic, "ch", handler);
        System.out.println("subscribed, waiting");
        Util.sleepQuietly(2000);

        assertEquals(received, Arrays.asList("message 1.", "new message 2."));
        subscriber.stop();
    }

    @Test
    public void testLookup() throws IOException {
        System.out.println("testLookup");
        received.clear();

        Config config = new Config();
        Subscriber subscriber = new Subscriber(config, 10, "127.0.0.1");
        subscriber.subscribe("first", "ch", handler);
        subscriber.subscribe("after", "ch", handler);

        postMessage("localhost:4151", "first", "msg one");
        Util.sleepQuietly(12000);

        assertEquals(received, Arrays.asList("msg one"));

        postMessage("localhost:4151", "after", "msg two");
        Util.sleepQuietly(12000);

        assertEquals(received, Arrays.asList("msg one", "msg two"));
        subscriber.stop();
    }
    */

    @Test
    public void testReconnect() throws Exception {
        System.out.println("testReconnect");
        received.clear();
        String topic = "t3";

        postMessage("localhost:4151", topic, "before disconnect");
        Util.sleepQuietly(2000);

        Config config = new Config();
        Subscriber subscriber = new Subscriber("127.0.0.1");
        subscriber.setLookupIntervalSecs(10);
        subscriber.subscribe(topic, "ch", handler);
        Util.sleepQuietly(2000);

        assertEquals(received, Arrays.asList("before disconnect"));
        exec("src/test/script/restart_nsqd.sh");

        postMessage("localhost:4151", topic, "after reconnect");
        Util.sleepQuietly(8000);

        assertEquals(received, Arrays.asList("before disconnect", "after reconnect"));
        subscriber.stop(0);
    }

    @AfterClass
    public static void afterTests() {
        System.out.println("after tests");
    }
}
