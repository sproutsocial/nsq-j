package com.sproutsocial.nsq;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

//TODO Delete me

public class MiscMain {

    private static Publisher loadPublisher;
    private static Random rand = new Random(83483839L);

    public static void handle(Message msg) {
        System.out.println("msg:" + new String(msg.getData()));
        Util.sleepQuietly(10000);
        msg.finish();
        System.out.println("finished:" + new String(msg.getData()));
    }

    public static void testPub() {
        Publisher publisher = new Publisher("localhost");
        for (int i = 0; i < 5; i++) {
            System.out.println("pub " + i);
            publisher.publish("cli2", ("pub test " + i).getBytes());
            Util.sleepQuietly(1000);
        }
        List<byte[]> toPub = new ArrayList<byte[]>();
        for (int i = 0; i < 10; i++) {
            String msg = "multi pub test " + i;
            toPub.add(msg.getBytes());

        }
        publisher.publish("cli2", toPub);
        System.out.println("multipub done");
        Util.sleepQuietly(1000);
        for (int i = 0; i < 100; i++) {
            System.out.println("pub " + i);
            publisher.publish("cli2", ("fast pub test " + i).getBytes());
        }
        System.out.println("fast pub done");
        Util.sleepQuietly(10000);
    }

    public static void testSubOld() {
        Config config = new Config();
        //config.setDeflate(true);
        //config.setDeflateLevel(6);
        config.setSnappy(true);
        Subscriber subscriber = new Subscriber(30, "localhost");
        subscriber.setDefaultMaxInFlight(10);
        System.out.println("created subscriber");

        subscriber.subscribe("cli3", "cli_test", new MessageHandler() {
            public void accept(Message msg) {
                handle(msg);
            }
        });
        System.out.println("subscribed");

        Util.sleepQuietly(120000);
        subscriber.stop();
    }

    public static void testSub() throws Exception {
        Config config = new Config();
        Client client = new Client();
        //config.setDeflate(true);
        //config.setDeflateLevel(6);
        //config.setSnappy(true);
        config.setMsgTimeout(25000);

        config.setTlsV1(true);
        client.setAuthSecret("test_local");
        client.setSSLSocketFactory(socketFactory());

        /*
        //DirectSubscriber subscriber = new DirectSubscriber(30, "localhost");
        Subscriber subscriber = new Subscriber(client, 30, "localhost");
        subscriber.setConfig(config);
        subscriber.setDefaultMaxInFlight(2);
        System.out.println("created subscriber");

        subscriber.subscribe("test1", "chan1", new MessageHandler() {
            public void accept(Message msg) {
                handle(msg);
            }
        });
        System.out.println("subscribed");
        */

        Util.sleepQuietly(1000);
        Publisher publisher = new Publisher(client, "localhost", null);
        publisher.setConfig(config);

        System.out.println("publishing");
        publisher.publish("test2", "from java".getBytes());
        System.out.println("published");
        //Util.sleepQuietly(30000);
        //subscriber.setMaxInFlightPerSubscription(1);

        Util.sleepQuietly(3000000);
        System.out.println("calling Client.stop");
        client.stop(30000);
        System.out.println("Client.stop done");
    }

    private static SSLSocketFactory socketFactory() throws Exception {
        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        InputStream in = new BufferedInputStream(new FileInputStream("/home/rob/workcode/infra-mine/nsq-j/src/test/resources/java_keystore.jks"));
        keyStore.load(in, "password".toCharArray());

        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(keyStore);
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(null, tmf.getTrustManagers(), null);
        return ctx.getSocketFactory();
    }

    public static void handleLoad(Message msg) {
        String text = new String(msg.getData());
        if (rand.nextFloat() < 0.5) {
            text = "PUB " + text;
            loadPublisher.publish("load-repub", text.getBytes());
        }
        if (rand.nextFloat() < 0.001) {
            System.out.println(text);
        }
        msg.finish();
    }

    public static void testLoad() {
        Config config = new Config();
        Subscriber subscriber = new Subscriber(30, "localhost");
        subscriber.setDefaultMaxInFlight(500);
        System.out.println("created subscriber");

        loadPublisher = new Publisher("localhost", "localhost:5150");
        System.out.println("created publisher");

        subscriber.subscribe("load", "test", new MessageHandler() {
            public void accept(Message msg) {
                handleLoad(msg);
            }
        });
        System.out.println("subscribed");

    }

    public static void main(String[] args) {
        try {
            //nohup nsqd -lookupd-tcp-address=localhost:4160 -broadcast-address=192.168.1.101 > log-nsqd 2>&1 &
            //curl -d 'message 1' 'http://localhost:4151/put?topic=cli'

            testSub();

            //testPub();

            //testLoad();

            Util.sleepQuietly(3000000);

            //List<String> prodLookups = Arrays.asList("nsq-lookup01", "nsq-lookup02", "nsq-lookup03");

            System.out.println("main done");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

}
