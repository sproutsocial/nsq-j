package com.sproutsocial.nsq;

import java.util.HashSet;
import java.util.Set;

/**
 * Subscribe from a given set of nsqd hosts instead of using the lookup service.
 */
public class DirectSubscriber extends Subscriber {

    private final Set<HostAndPort> nsqds = new HashSet<HostAndPort>();

    public DirectSubscriber(int checkHostsIntervalSecs, String... nsqdHosts) {
        super(checkHostsIntervalSecs);
        for (String h : nsqdHosts) {
            nsqds.add(HostAndPort.fromString(h).withDefaultPort(4150));
        }
    }

    @Override
    protected Set<HostAndPort> lookupTopic(String topic) {
        return nsqds;
    }

}
