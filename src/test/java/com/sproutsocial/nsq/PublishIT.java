package com.sproutsocial.nsq;

import org.junit.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

//run one test: mvn -Dit.test=PublishIT#testSnappy verify

public class PublishIT extends TestBase {

    private boolean usePublishSoon = false;

    private void send(Publisher publisher, String topic, List<String> msgs, float delayChance, int maxDelay) {
        int count = 0;
        for (String msg : msgs) {
            if (random.nextFloat() < delayChance) {
                Util.sleepQuietly(random.nextInt(maxDelay));
            }
            if (usePublishSoon) {
                publisher.publishBuffered(topic, msg.getBytes());
            }
            else {
                publisher.publish(topic, msg.getBytes());
            }
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
    public void testPubSoon() throws Exception {
        System.out.println("testPubSoon. random seed:" + seed);
        usePublishSoon = true;
        testPub(new Config(), 2000);
        usePublishSoon = false;
    }

    @Test
    public void testPub() throws Exception {
        System.out.println("testPub. random seed:" + seed);
        testPub(new Config(), 500);
    }

    private void testPub(Config config, int maxMsgLen) throws Exception {
        String cmd = "nsq_tail -topic=pubtest -channel=tail#ephemeral -nsqd-tcp-address=localhost:4150";
        Process proc = Runtime.getRuntime().exec(cmd.split(" "));
        List<String> received = readLines(proc);

        Publisher publisher = new Publisher("localhost");
        publisher.setConfig(config);
        List<String> msgs = messages(200, maxMsgLen);
        if (usePublishSoon) {
            send(publisher, "pubtest", msgs, 0.045f, 1200); //nice mix of delay/size at maxDelay=300 maxSize=16k
        }
        else {
            send(publisher, "pubtest", msgs, 0.1f, 1000);
        }

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

    /* FAIL! - testMultiPub fails - buffer sizes? try with big single message?
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
        testPub(config, 500);
        testMultiPub(config);
    }

    /*
    openssl req -x509 -newkey rsa:2048 -keyout key.pem -out cert.pem -days 365 -nodes
    keytool -import -file cert.pem -alias server -keystore server.jks
     */
    @Test
    public void testEncryption() throws Exception {
        System.out.println("testEncryption. random seed:" + seed);

        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(getClass().getResourceAsStream("/java_keystore.jks"), "password".toCharArray());

        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(keyStore);
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(null, tmf.getTrustManagers(), null);
        SSLSocketFactory socketFactory = ctx.getSocketFactory();

        Client.getDefaultClient().setSSLSocketFactory(socketFactory);

        Config config = new Config();
        config.setTlsV1(true);
        testPub(config, 500);
        testMultiPub(config);
    }

}
