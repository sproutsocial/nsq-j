package com.sproutsocial.nsq;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.slf4j.LoggerFactory.getLogger;

public class RoundRobinDockerTestIT extends BaseDockerTestIT {
    private static final Logger LOGGER = getLogger(RoundRobinDockerTestIT.class);
    private Subscriber subscriber;
    private Publisher publisher;
    private TestMessageHandler handler;

    @Override
    public void setup() {
        super.setup();
        Util.sleepQuietly(500);
        //This needs to be crazy long because it can take up to 1 min for the connections in the subscriber to timeout and reconnect.
        handler = new TestMessageHandler(60_000);
        subscriber = new Subscriber(client, 1, 50, cluster.getLookupNode().getHttpHostAndPort().toString());
        subscriber.subscribe(topic, "tail" + System.currentTimeMillis(), handler);
        publisher = roundRobinPublisher();
    }

    @Override
    public void teardown() throws InterruptedException {
        subscriber.stop();
        if (publisher != null) {
            publisher.stop();
        }
        super.teardown();
    }


    @Test
    public void test_happyPath() {
        publishAndValidateRoundRobinForNodes(cluster.getNsqdNodes(), 0);
    }

    private void validateMessagesSentRoundRobin(List<NsqDockerCluster.NsqdNode> nsqdNodes, int count, List<String> messages, List<NSQMessage> receivedMessages, int nodeOffset) {
        Map<String, List<NSQMessage>> messagesByNsqd = mapByNsqd(receivedMessages);
        for (int i = 0; i < count; i++) {
            String nsqdHst = nsqdNodes.get((i + nodeOffset) % nsqdNodes.size()).getTcpHostAndPort().toString();
            int expectedIndex = i / nsqdNodes.size();
            String nsqdMessage = new String(messagesByNsqd.get(nsqdHst).get(expectedIndex).getData());
            assertEquals("For message " + i + " expect it to be from nsqd " + nsqdHst, messages.get(i), nsqdMessage);
        }
    }

    @Test
    public void test_singleNodeFailed() {
        List<NsqDockerCluster.NsqdNode> nsqdNodes = cluster.getNsqdNodes();
        cluster.disconnectNetworkFor(nsqdNodes.get(2));
        publishAndValidateRoundRobinForNodes(nsqdNodes.subList(0, 2), 0);
    }

    @Test
    public void test_SingleNodeFailsAndRecovers() {
        List<NsqDockerCluster.NsqdNode> nsqdNodes = cluster.getNsqdNodes();
        cluster.disconnectNetworkFor(nsqdNodes.get(2));
        publishAndValidateRoundRobinForNodes(nsqdNodes.subList(0, 2), 0);
        cluster.reconnectNetworkFor(cluster.getNsqdNodes().get(2));
        Util.sleepQuietly(5000);
        publishAndValidateRoundRobinForNodes(nsqdNodes, 2);
    }

    private void publishAndValidateRoundRobinForNodes(List<NsqDockerCluster.NsqdNode> nsqdNodes, int nodeOffset) {
        int count = 10 * nsqdNodes.size();
        List<String> messages = messages(count, 40);
        send(topic, messages, 0.5f, 10, publisher);
        List<NSQMessage> receivedMessages = handler.drainMessagesOrTimeOut(messages.size());
        validateReceivedAllMessages(messages, receivedMessages, false);
        validateMessagesSentRoundRobin(nsqdNodes, count, messages, receivedMessages, nodeOffset);
    }

    @Test(timeout = 500)
    public void test_allNodesDown_throwsException() {
        for (NsqDockerCluster.NsqdNode nsqdNode : cluster.getNsqdNodes()) {
            cluster.disconnectNetworkFor(nsqdNode);
        }
        int count = 1;
        List<String> messages = messages(count, 40);
        Assert.assertThrows(NSQException.class, () -> send(topic, messages, 0.5f, 10, publisher));
    }


    @Test()
    public void test_allNodesDown_LaterRecovers() {
        publishAndValidateRoundRobinForNodes(cluster.getNsqdNodes(), 0);

        for (NsqDockerCluster.NsqdNode nsqdNode : cluster.getNsqdNodes()) {
            cluster.disconnectNetworkFor(nsqdNode);
        }
        int count = 50;
        List<String> messages = messages(count, 40);
        Assert.assertThrows(NSQException.class, () -> send(topic, messages, 0.5f, 10, publisher));
        LOGGER.info(subscriber.toString());

        for (NsqDockerCluster.NsqdNode nsqdNode : cluster.getNsqdNodes()) {
            cluster.reconnectNetworkFor(nsqdNode);
        }

        Util.sleepQuietly(6000);

        // Explicitly recreate the subscrber to get fresh connections.  Otherwise we would need to wait for the socket timeout of 60 seconds
        subscriber.stop();
        subscriber = new Subscriber(client, 1, 50, cluster.getLookupNode().getHttpHostAndPort().toString());
        subscriber.subscribe(topic, "tail" + System.currentTimeMillis(), handler);

        Assert.assertTrue(handler.drainMessages(1).isEmpty());

        send(topic, messages, 0.1f, 50, publisher);
        List<NSQMessage> nsqMessages = handler.drainMessagesOrTimeOut(count);
        validateReceivedAllMessages(messages, nsqMessages, false);
    }


    @Test()
    public void test_twoNodesDown_LaterRecovers() {
        publishAndValidateRoundRobinForNodes(cluster.getNsqdNodes(), 0);

        cluster.disconnectNetworkFor(cluster.getNsqdNodes().get(0));
        cluster.disconnectNetworkFor(cluster.getNsqdNodes().get(1));

        publishAndValidateRoundRobinForNodes(cluster.getNsqdNodes().subList(2, 3), 0);

        cluster.reconnectNetworkFor(cluster.getNsqdNodes().get(0));
        cluster.reconnectNetworkFor(cluster.getNsqdNodes().get(1));

        Util.sleepQuietly(6000);

        Assert.assertTrue(handler.drainMessages(1).isEmpty());

        publishAndValidateRoundRobinForNodes(cluster.getNsqdNodes(), 0);
    }

    @Test
    public void test_oneNodeDown_FirstPublishDeferredThrowsException() {
        publishAndValidateRoundRobinForNodes(cluster.getNsqdNodes(),0);
        cluster.disconnectNetworkFor(cluster.getNsqdNodes().get(0));
        List<String> messages = messages(1, 20);
        Assert.assertThrows(NSQException.class, () -> publisher.publishDeferred(topic, messages.get(0).getBytes(), 10, TimeUnit.MILLISECONDS));
    }

    @Test
    public void test_oneNodeDown_DeferredWithRetryWillRetry() {
        publishAndValidateRoundRobinForNodes(cluster.getNsqdNodes(),0);

        cluster.disconnectNetworkFor(cluster.getNsqdNodes().get(0));
        List<String> messages = messages(1, 20);
        publisher.publishDeferredWithRetry(topic, messages.get(0).getBytes(), 10, TimeUnit.MILLISECONDS);

        List<NSQMessage> nsqMessages = handler.drainMessagesOrTimeOut(1);
        assertEquals(messages.get(0),new String(nsqMessages.get(0).getData()));

    }


}
