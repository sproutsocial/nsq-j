package com.sproutsocial.nsq;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class RoundRobinDockerTestIT extends BaseDockerTestIT {

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

    private void validateMessagesSentRoundRobin(List<NsqDockerCluster.NsqdNode> nsqdNodes, int count, List<String> messages, List<NSQMessage> receivedMessages) {
        validateMessagesSentRoundRobin(nsqdNodes, count, messages, receivedMessages, 0);
    }

    private void validateMessagesSentRoundRobin(List<NsqDockerCluster.NsqdNode> nsqdNodes, int count, List<String> messages, List<NSQMessage> receivedMessages, int nodeOffset) {
        Map<String, List<NSQMessage>> messagesByNsqd = mapByNsqd(receivedMessages);
        for (int i = 0; i < count; i++) {
            String nsqdHst = nsqdNodes.get((i+ nodeOffset) % nsqdNodes.size()).getTcpHostAndPort().toString();
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
    public void test_SingleNodeFailsAndRecovers(){
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
    public void test_allNodesDown_throwsException(){
        for (NsqDockerCluster.NsqdNode nsqdNode : cluster.getNsqdNodes()) {
            cluster.disconnectNetworkFor(nsqdNode);
        }
        int count = 1;
        List<String> messages = messages(count, 40);
        Assert.assertThrows(NSQException.class,()-> send(topic, messages, 0.5f, 10, publisher));
    }

    @Test()
    public void test_allNodesDown_LaterRecovers(){
        for (NsqDockerCluster.NsqdNode nsqdNode : cluster.getNsqdNodes()) {
            cluster.disconnectNetworkFor(nsqdNode);
        }
        int count = 1;
        List<String> messages = messages(count, 40);
        Assert.assertThrows(NSQException.class,()-> send(topic, messages, 0.5f, 10, publisher));

        for (NsqDockerCluster.NsqdNode nsqdNode : cluster.getNsqdNodes()) {
            cluster.reconnectNetworkFor(nsqdNode);
        }

        Util.sleepQuietly(5000);

        Assert.assertTrue(handler.drainMessages(1).isEmpty());

        send(topic, messages, 0.5f, 10, publisher);
        List<NSQMessage> nsqMessages = handler.drainMessagesOrTimeOut(1);
        assertEquals(messages.get(0),new String(nsqMessages.get(0).getData()));
    }


}
