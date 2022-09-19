package com.sproutsocial.nsq;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.slf4j.LoggerFactory.getLogger;

public class BaseDockerTestIT {
    protected NsqDockerCluster cluster;
    protected String topic;
    protected ScheduledExecutorService scheduledExecutorService;
    private static final Logger LOGGER = getLogger(BaseDockerTestIT.class);
    protected Client client;

    private final AtomicInteger messageBatchCounter = new AtomicInteger();

    @Before
    public void setup() {
        client = new Client();
        //A single threaded executor preserves ordering
        client.setExecutor(Executors.newSingleThreadExecutor());
        cluster = NsqDockerCluster.builder()
                .withNsqdCount(3)
                .start()
                .awaitExposedPorts();

        topic = "topic" + System.currentTimeMillis();

        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    }

    @After
    public void teardown() throws InterruptedException {
        client.stop();

        cluster.shutdown();

        scheduledExecutorService.shutdown();
        scheduledExecutorService.awaitTermination(10, TimeUnit.SECONDS);
    }

    protected void send(String topic, List<String> msgs, double delayChance, int maxDelay, Publisher publisher) {
        int count = 0;
        LOGGER.info("Sending {} messags to topic {}", msgs.size(), topic);
        Random random = new Random();
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

    protected List<String> messages(int count, int len) {
        List<String> res = new ArrayList<>(count);
        int batch = messageBatchCounter.getAndIncrement();
        Random random = new Random();
        for (int i = 0; i < count; i++) {
            StringBuilder s = new StringBuilder();
            s.append(String.format("msg %04d batch: %04d len:%04d ", i, batch, len));
            for (int j = 0; j < len; j++) {
                s.append((char) (33 + random.nextInt(92)));
            }
            res.add(s.toString());
        }
        return res;
    }

    protected void sendAndVerifyMessagesFromBackup(Publisher publisher, TestMessageHandler handler) {
        List<String> postFailureMessages = messages(20, 40);
        send(topic, postFailureMessages, 0.5, 10, publisher);
        List<NSQMessage> receivedMessages = handler.drainMessagesOrTimeOut(postFailureMessages.size());
        validateReceivedAllMessages(postFailureMessages, receivedMessages, true);
        validateFromParticularNsqd(receivedMessages, 1);
    }

    protected void sendAndVerifyMessagesFromPrimary(Publisher publisher, TestMessageHandler handler) {
        List<String> messages = messages(20, 40);
        send(topic, messages, 0.5f, 10, publisher);
        List<NSQMessage> receivedMessages = handler.drainMessagesOrTimeOut(messages.size());
        validateReceivedAllMessages(messages, receivedMessages, true);
        validateFromParticularNsqd(receivedMessages, 0);
    }

    protected void validateFromParticularNsqd(List<NSQMessage> receivedMessages, int nsqHostIndex) {
        for (NSQMessage e : receivedMessages) {
            Assert.assertEquals(cluster.getNsqdNodes().get(nsqHostIndex).getTcpHostAndPort(), e.getConnection().getHost());
        }
    }

    protected Publisher primaryOnlyPublisher() {
        return new Publisher(client, cluster.getNsqdNodes().get(0).getTcpHostAndPort().toString(), null);
    }

    protected Publisher backupPublisher() {
        Publisher publisher = new Publisher(client, cluster.getNsqdNodes().get(0).getTcpHostAndPort().toString(), cluster.getNsqdNodes().get(1).getTcpHostAndPort().toString());
        publisher.setFailoverDurationSecs(5);
        return publisher;
    }

    public void validateReceivedAllMessages(List<String> expected, List<NSQMessage> actual, boolean validateOrder) {
        List<String> actualMessages = actual.stream().map(m -> new String(m.getData())).collect(Collectors.toList());
        List<String> expectedCopy = new ArrayList<>(expected);
        if (!validateOrder) {
            Collections.sort(actualMessages);
            Collections.sort(expectedCopy);
        } else {
            validateReceivedAllMessages(expected, actual, false);
            LOGGER.info("Validated that all the messages are there first before validating order");
            if (actualMessages.size() < 100) {
                LOGGER.info("Received messages in receive order: {}", actualMessages);
            }
        }
        Assert.assertArrayEquals("Validation with ordering expected?" + validateOrder, expected.toArray(), actualMessages.toArray());
    }
}
