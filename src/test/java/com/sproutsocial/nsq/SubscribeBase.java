package com.sproutsocial.nsq;

import com.google.common.base.Joiner;
import org.junit.Before;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SubscribeBase extends TestBase {

    protected final List<String> received = Collections.synchronizedList(new ArrayList<String>());
    protected final MessageHandler handler;

    public SubscribeBase() {
        handler = new MessageHandler() {
            public void accept(Message msg) {
                handle(msg);
            }
        };
    }

    @Before
    public void beforeTest() {
        received.clear();
    }

    protected void post(String host, String topic, String command,  String body) throws IOException {
        URL url = new URL(String.format("http://%s/%s?topic=%s", host, command, URLEncoder.encode(topic, "UTF-8")));
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("POST");
        con.setDoOutput(true);
        con.getOutputStream().write(body.getBytes());
        con.getOutputStream().close();
        System.out.println("post resp:" + con.getResponseCode() + " " + con.getResponseMessage());
    }

    protected void postMessages(String host, String topic, List<String> msgs) throws IOException {
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

    protected void handle(Message msg) {
        received.add(new String(msg.getData()));
        msg.finish();
    }

}
