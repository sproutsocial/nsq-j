package com.sproutsocial.nsq;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class PublishIT extends TestBase {


    @BeforeClass
    public static void beforeTests() throws Exception {
        exec("src/test/script/start_tests.sh");
    }

    private void send(Publisher publisher, String topic, List<String> msgs, float delayChance, int maxDelay) {
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

    private List<String> readLines(final Process proc) {
        final List<String> received = Collections.synchronizedList(new ArrayList<String>());
        new Thread() {
            public void run() {
                try {
                    BufferedReader in = new BufferedReader(new InputStreamReader(proc.getInputStream()));
                    String line = in.readLine();
                    while (line != null) {
                        //System.out.println("received:" + line);
                        received.add(line);
                        line = in.readLine();
                    }
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }.start();
        return received;
    }

    @Test
    public void testPub() throws Exception {
        System.out.println("testPub. random seed:" + seed);
        testPub(new Config());
    }

    private void testPub(Config config) throws Exception {
        String cmd = "nsq_tail -topic=pubtest -channel=tail#ephemeral -nsqd-tcp-address=localhost:4150";
        Process proc = Runtime.getRuntime().exec(cmd.split(" "));
        List<String> received = readLines(proc);

        Publisher publisher = new Publisher("localhost");
        publisher.setConfig(config);
        List<String> msgs = messages(200, 500);
        send(publisher, "pubtest", msgs, 0.1f, 1000);

        Util.sleepQuietly(1000);
        proc.destroy();

        assertEquals(msgs, received);
    }

    @Test
    public void testMultiPub() throws Exception {
        System.out.println("testMultiPub. random seed:" + seed);
        testMultiPub(new Config());
    }

    private void testMultiPub(Config config) throws Exception {
        String cmd = "nsq_tail -topic=mpubtest -channel=tail#ephemeral -nsqd-tcp-address=localhost:4150";
        Process proc = Runtime.getRuntime().exec(cmd.split(" "));
        List<String> received = readLines(proc);

        Publisher publisher = new Publisher("localhost");
        publisher.setConfig(config);

        List<String> sent = new ArrayList<String>();

        int count = 0;
        for (int i = 0; i < 10; i++) {
            List<String> msgs = messages(10 + random.nextInt(100), 500);
            List<byte[]> msgData = new ArrayList<byte[]>(msgs.size());
            for (String msg : msgs) {
                msgData.add(msg.getBytes());
            }
            publisher.publish("mpubtest", msgData);
            count += msgs.size();
            sent.addAll(msgs);
            System.out.println("mpub " + count);
            if (i % 3 == 0) {
                Util.sleepQuietly(random.nextInt(2000));
            }
        }

        Util.sleepQuietly(1000);
        proc.destroy();

        assertEquals(sent, received);
    }

    /* FAIL!
    @Test
    public void testSnappy() throws Exception {
        System.out.println("testSnappy. random seed:" + seed);
        Config config = new Config();
        config.setSnappy(true);
        testPub(config);
        testMultiPub(config);
    }
    */

    @Test
    public void testDeflate() throws Exception {
        System.out.println("testDeflate. random seed:" + seed);
        Config config = new Config();
        config.setDeflate(true);
        testPub(config);
        testMultiPub(config);
    }

    private void debugFail(List<String> msgs, List<String> received) {
        assertEquals(msgs.size(), received.size());
        for (int i = 0; i < msgs.size(); i++) {
            System.out.println(msgs.get(i));
            System.out.println(received.get(i));
            assertEquals(msgs.get(i), received.get(i));
            System.out.println();
        }
    }

}
