package com.sproutsocial.nsq;

import java.util.ArrayList;
import java.util.List;

public class MiscMain {

    public static void handle(Message msg) {
        System.out.println("msg:" + new String(msg.getData()));
        Util.sleepQuietly(1000);
        msg.finish();
        System.out.println("  finished");
    }

    public static void testPub() {
        Config config = new Config();
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

    public static void testSub() {
        Config config = new Config();
        //config.setDeflate(true);
        //config.setDeflateLevel(6);
        config.setSnappy(true);
        Subscriber subscriber = new Subscriber("localhost");
        subscriber.setLookupIntervalSecs(30);
        subscriber.setMaxInFlightPerSubscription(10);
        System.out.println("created subscriber");

        subscriber.subscribe("cli3", "cli_test", new MessageHandler() {
            public void accept(Message msg) {
                handle(msg);
            }
        });
        System.out.println("subscribed");

        Util.sleepQuietly(120000);
        subscriber.stop(0);
    }

    public static void main(String[] args) {
        try {
            //nohup nsqd -lookupd-tcp-address=localhost:4160 -broadcast-address=192.168.1.101 > log-nsqd 2>&1 &
            //curl -d 'message 1' 'http://localhost:4151/put?topic=cli'

            //testPub();

            testSub();

            //List<String> prodLookups = Arrays.asList("nsq-lookup01", "nsq-lookup02", "nsq-lookup03");

            System.out.println("done");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

}
