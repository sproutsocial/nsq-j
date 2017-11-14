package com.sproutsocial.nsq;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.DataOutputStream;
import java.util.*;

import static org.junit.Assert.*;
import static org.powermock.api.mockito.PowerMockito.methods;
import static org.powermock.api.mockito.PowerMockito.suppress;

//to prepare multiple classes
//@PrepareForTest({SubConnection.class, Client.class})

@RunWith(PowerMockRunner.class)
@PrepareForTest(SubConnection.class)
@PowerMockIgnore({"javax.management.*"})
public class SubscriptionTest {

    @Before
    public void beforeClass() {
        suppress(methods(SubConnection.class, "connect"));
        suppress(methods(SubConnection.class, "writeCommand"));
        suppress(methods(SubConnection.class, "flush"));
        suppress(methods(SubConnection.class, "close"));
        suppress(methods(SubConnection.class, "scheduleAtFixedRate"));
    }

    @Test
    public void testDistributeInFlight() throws Exception {
        Client client = new Client();
        Subscriber subscriber = new Subscriber(client, 30, 5);
        Subscription sub = new Subscription(client, "topic", "channel", null, subscriber, 200);

        testInFlight(1, 1, sub, 200);
        sub = new Subscription(client, "topic", "channel", null, subscriber, subscriber.getDefaultMaxInFlight());
        testInFlight(2, 2, sub, 100, 100);
        sub = new Subscription(client, "topic", "channel", null, subscriber, subscriber.getDefaultMaxInFlight());
        testInFlight(2, 1, sub, 199);
        sub = new Subscription(client, "topic", "channel", null, subscriber, subscriber.getDefaultMaxInFlight());
        testInFlight(3, 3, sub, 66, 67, 67);
        sub = new Subscription(client, "topic", "channel", null, subscriber, subscriber.getDefaultMaxInFlight());
        testInFlight(5, 3, sub, 66, 66, 66);

        subscriber.setDefaultMaxInFlight(2500);
        sub = new Subscription(client, "topic", "channel", null, subscriber, subscriber.getDefaultMaxInFlight());
        testInFlight(4, 4, sub, 625, 625, 625, 625);
        sub = new Subscription(client, "topic", "channel", null, subscriber, subscriber.getDefaultMaxInFlight());
        testInFlight(6, 6, sub, 417, 417, 417, 417, 416, 416);
        sub = new Subscription(client, "topic", "channel", null, subscriber, subscriber.getDefaultMaxInFlight());
        testInFlight(7, 4, sub, 624, 624, 624, 625);
        sub = new Subscription(client, "topic", "channel", null, subscriber, subscriber.getDefaultMaxInFlight());
        testInFlight(10, 6, sub, 416, 416, 416, 416, 416, 416);
    }

    private void testInFlight(int numCons, int numActive, Subscription sub, Integer... activeInFlight) {
        Map<HostAndPort, SubConnection> conMap = Whitebox.getInternalState(sub, "connectionMap");
        for (SubConnection con : conMap.values()) {
            con.lastActionFlush = 5000; //so connections get removed during checkConnections
        }
        checkHosts(sub, numCons);
        for (SubConnection con : conMap.values()) {
            con.out = Mockito.mock(DataOutputStream.class);
        }
        Set<SubConnection> allCons = new HashSet<SubConnection>(conMap.values());
        Set<SubConnection> active = setActive(numActive, allCons);
        Set<SubConnection> inactive = Sets.difference(allCons, active);
        checkHosts(sub, numCons);
        assertFalse(sub.isLowFlight());
        assertEquals(numCons, conMap.size());
        assertEquals(Sets.union(active, inactive), new HashSet<SubConnection>(conMap.values()));

        for (SubConnection con : inactive) {
            assertEquals(1, con.getMaxInFlight());
        }
        Multiset<Integer> expectedMaxInFlight = HashMultiset.create(Arrays.asList(activeInFlight));
        Multiset<Integer> actualMaxInFlight = HashMultiset.create();
        for (SubConnection con : active) {
            actualMaxInFlight.add(con.getMaxInFlight());
        }
        assertEquals(expectedMaxInFlight, actualMaxInFlight);
    }

    private void checkHosts(Subscription sub, int count) {
        Set<HostAndPort> hosts = new HashSet<HostAndPort>();
        for (int i = 0; i < count; i++) {
            hosts.add(HostAndPort.fromParts("host" + i, 123));
        }
        sub.checkConnections(hosts);
    }

    private Set<SubConnection> setActive(int numActive, Collection<SubConnection> cons) {
        Set<SubConnection> activeSet = new HashSet<SubConnection>();
        for (SubConnection con : cons) {
            if (activeSet.size() < numActive) {
                con.lastActionFlush = Util.clock() - 1000;
                activeSet.add(con);
            }
            else {
                con.lastActionFlush = 5000;
            }
        }
        return activeSet;
    }

    @Test
    public void testLowFlight() throws Exception {
        Client client = new Client();
        Subscriber subscriber = new Subscriber(client, 30, 5);
        Subscription sub = new Subscription(client, "topic", "channel", null, subscriber, 100);

        testLowFlight(subscriber, sub, 2, 3);
        sub = new Subscription(client, "topic", "channel", null, subscriber, subscriber.getDefaultMaxInFlight());
        testLowFlight(subscriber, sub, 1, 2);
        sub = new Subscription(client, "topic", "channel", null, subscriber, subscriber.getDefaultMaxInFlight());
        testLowFlight(subscriber, sub, 5, 10);
    }

    private Set<SubConnection> getReady(Collection<SubConnection> cons) {
        Set<SubConnection> ready = new HashSet<SubConnection>();
        for (SubConnection con : cons) {
            //System.out.println(con.getHost() + " maxInFlight:" + con.getMaxInFlight());
            if (con.getMaxInFlight() != 0) {
                ready.add(con);
            }
        }
        return ready;
    }

    private void testLowFlight(Subscriber subscriber, Subscription sub, int maxInflight, int numCons) throws Exception {
        sub.setMaxInFlight(maxInflight);
        checkHosts(sub, numCons);
        assertTrue(sub.isLowFlight());
        Map<HostAndPort, SubConnection> conMap = Whitebox.getInternalState(sub, "connectionMap");

        Set<SubConnection> ready = getReady(conMap.values());
        Set<SubConnection> paused = Sets.difference(new HashSet<SubConnection>(conMap.values()), ready);

        assertEquals(maxInflight, ready.size());
        assertEquals(numCons - maxInflight, paused.size());

        Whitebox.invokeMethod(sub, "rotateLowFlight");

        Set<SubConnection> nextReady = getReady(conMap.values());
        Set<SubConnection> nextPaused = Sets.difference(new HashSet<SubConnection>(conMap.values()), nextReady);

        assertEquals(maxInflight, nextReady.size());
        assertEquals(numCons - maxInflight, nextPaused.size());
        assertEquals(1, Sets.difference(ready, nextReady).size());
        assertEquals(1, Sets.difference(paused, nextPaused).size());
    }

}
