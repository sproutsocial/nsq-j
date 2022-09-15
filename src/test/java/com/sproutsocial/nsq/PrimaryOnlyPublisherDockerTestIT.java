package com.sproutsocial.nsq;

import org.junit.Assert;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import java.security.KeyStore;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class PrimaryOnlyPublisherDockerTestIT extends BaseDockerTestIT {
    private Subscriber subscriber;
    private Publisher publisher;
    private TestMessageHandler handler;

    @Override
    public void setup() {
        super.setup();
        Util.sleepQuietly(500);
        handler = new TestMessageHandler();
        subscriber = new Subscriber(client, 1, 5, cluster.getLookupNode().getHttpHostAndPort().toString());
        subscriber.subscribe(topic, "tail" + System.currentTimeMillis(), handler);
        publisher = primaryOnlyPublisher();
    }

    @Override
    public void teardown() throws InterruptedException {
        if (subscriber != null) {
            subscriber.stop();
        }
        if (publisher != null) {
            publisher.stop();
        }
        super.teardown();
    }


    @Test
    public void testBasicRoundTrip() {
        sendAndVerifyMessagesFromPrimary(publisher, handler);
    }

    @Test
    public void testBatcherPublish_noBackup() {
        // This method tests both the batcher/buffered publish as well as the arraylist publish
        List<String> messages = messages(20, 40);
        for (String message : messages) {
            publisher.publishBuffered(topic, message.getBytes());
        }
        List<NSQMessage> receivedMessages = handler.drainMessagesOrTimeOut(messages.size());
        validateReceivedAllMessages(messages, receivedMessages, true);
    }

    @Test
    public void testDeferredPublish_noBackup() {
        int delayMillis = 500;
        List<String> messages = messages(5, 40);
        for (String message : messages) {
            publisher.publishDeferred(topic, message.getBytes(), delayMillis, TimeUnit.MILLISECONDS);
        }
        Util.sleepQuietly(delayMillis / 2);
        List<NSQMessage> shouldBeEmpty = handler.drainMessages(messages.size());
        Assert.assertTrue("Not expecting any messages yet since we are publishing defered", shouldBeEmpty.isEmpty());

        Util.sleepQuietly(delayMillis / 2);
        List<NSQMessage> receivedMessages = handler.drainMessagesOrTimeOut(messages.size());
        validateReceivedAllMessages(messages, receivedMessages, true);
    }

    @Test
    public void testSnappy() {
        publisher.getConfig().setSnappy(true);
        sendAndVerifyMessagesFromPrimary(publisher, handler);
    }

    @Test
    public void testDeflate() {
        publisher.getConfig().setDeflate(true);
        sendAndVerifyMessagesFromPrimary(publisher, handler);
    }

    /*
    openssl req -x509 -newkey rsa:2048 -keyout key.pem -out cert.pem -days 365 -nodes
    keytool -import -file cert.pem -alias server -keystore server.jks
     */
    @Test
    public void testEncryption() throws Exception {
        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(getClass().getResourceAsStream("/java_keystore.jks"), "password".toCharArray());

        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(keyStore);
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(null, tmf.getTrustManagers(), null);
        SSLSocketFactory socketFactory = ctx.getSocketFactory();

        client.setSSLSocketFactory(socketFactory);
        publisher.getConfig().setTlsV1(true);

        sendAndVerifyMessagesFromPrimary(publisher, handler);
    }



    @Test
    public void testBasicRoundTrip_noBackup_primaryNsqDown_waitBeforeTryingAgain() {
        cluster.disconnectNetworkFor(cluster.getNsqdNodes().get(0));

        List<String> messages = messages(20, 40);

        long startTimeMillis = System.currentTimeMillis();
        Assert.assertThrows(NSQException.class, () -> send(topic, messages, 0.5f, 10, publisher));
        long totalTimeMillis = System.currentTimeMillis() - startTimeMillis;
        Assert.assertTrue("Waited at least 10 seconds", totalTimeMillis > TimeUnit.SECONDS.toMillis(10));
    }


    @Test
    public void testBasicRoundTrip_noBackup_primaryNsqDownThenRecovers() {
        cluster.disconnectNetworkFor(cluster.getNsqdNodes().get(0));
        scheduledExecutorService.schedule(() -> cluster.reconnectNetworkFor(cluster.getNsqdNodes().get(0)), 1, TimeUnit.SECONDS);

        sendAndVerifyMessagesFromPrimary(publisher, handler);
    }
}
