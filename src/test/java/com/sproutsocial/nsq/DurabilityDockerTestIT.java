package com.sproutsocial.nsq;

import org.junit.Test;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;

import static org.slf4j.LoggerFactory.getLogger;

public class DurabilityDockerTestIT extends BaseDockerTestIT {
    private Publisher publisher;
    private TestMessageHandler handler1;
    private Subscriber subscriber;

    private static final Logger LOGGER = getLogger(DurabilityDockerTestIT.class);

    @Override
    public void setup() {
        super.setup();
        publisher = this.backupPublisher();
        // We don't need to force flush because we aren't failing over
        handler1 = new TestMessageHandler();
        subscriber = new Subscriber(client, 1, 10, cluster.getLookupNode().getHttpHostAndPort().toString());
        subscriber.setDefaultMaxInFlight(1000);
        subscriber.subscribe(topic, "channelA", handler1);
    }

    /**
     * Goal here is two fold:
     * 1. Really hammer the publisher and consumer side of things.  Make sure ordering is preserved as expected.
     * 2. Get some very rough numbers on throughput. They are really just there to identify regressions in other
     * publishing implementations.
     */
    @Test
    public void largeMessageVolumeTest_buffered() {
        int batches = 15;
        int batchSize = 5_000;
        List<Long> sendReceiveTimings = new ArrayList<>();
        List<Long> sendTimings = new ArrayList<>();
        for(int i = 0; i< batches; i++) {
            List<String> messages = messages(batchSize, 100);
            long startTime = System.currentTimeMillis();
            for (String message : messages) {
                publisher.publishBuffered(topic, message.getBytes());
            }
            publisher.flushBatchers();
            sendTimings.add(System.currentTimeMillis()-startTime);
            List<NSQMessage> nsqMessages = handler1.drainMessagesOrTimeOut(batchSize);
            sendReceiveTimings.add(System.currentTimeMillis()-startTime);

            validateReceivedAllMessages(messages,nsqMessages,true);
            LOGGER.info("published batch {} of {}",i,batches);
        }

        double sendAverageMillis = sendTimings.stream().mapToLong(e -> e).average().getAsDouble();
        double throughput = batchSize/sendAverageMillis*1000;
        LOGGER.info("Average send time for {} messages: {} millis, {} op/s", batchSize, sendAverageMillis, throughput);

        double totalAverageMillis = sendReceiveTimings.stream().mapToLong(e -> e).average().getAsDouble();
        throughput = batchSize/totalAverageMillis*1000;
        LOGGER.info("Average send time for {} messages: {} millis, {} op/s", batchSize, totalAverageMillis, throughput);
    }

    @Override
    public void teardown() throws InterruptedException {
        publisher.stop();
        subscriber.stop();
        super.teardown();
    }
}
