package com.sproutsocial.nsq;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.methods;
import static org.powermock.api.mockito.PowerMockito.suppress;

@RunWith(PowerMockRunner.class)
@PrepareForTest(SubConnection.class)
@PowerMockIgnore({"javax.management.*"})
public class SubscriptionInvalidHostTest {

    @Before
    public void beforeClass() {
        suppress(methods(SubConnection.class, "writeCommand"));
        suppress(methods(SubConnection.class, "flush"));
        suppress(methods(SubConnection.class, "scheduleAtFixedRate"));
    }

    @Test
    public void testInvalidNsqdHost__allConnectionsClosed() {
        ObservedConnectionClient client = new ObservedConnectionClient();

        try {
            // must implement Subscriber because cannot mock package protected getConfig()
            Subscriber subscriber = new Subscriber(client, 5, 5);
            Subscription subscription = new Subscription(client, "topic", "channel", null, subscriber, 1);

            Set<HostAndPort> activeHosts = new HashSet<HostAndPort>(
                    Collections.singletonList(HostAndPort.fromString("this-is-unknown-hostname:5555")));
            subscription.checkConnections(activeHosts);

            try {
                Thread.sleep(3000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            assertEquals(0, subscription.getConnectionCount());
            assertTrue(client.allSubConnectionsClosed());
        }
        finally {
            assertTrue(client.stop());
        }
    }
}
