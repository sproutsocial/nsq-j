package com.sproutsocial.nsq;

import com.google.common.net.HostAndPort;

import java.util.HashSet;
import java.util.Set;

public class DirectSubscriber extends Subscriber {

    private final Set<HostAndPort> nsqds = new HashSet<HostAndPort>();

    public DirectSubscriber(String... nsqdHosts) {
        for (String h : nsqdHosts) {
            nsqds.add(HostAndPort.fromString(h).withDefaultPort(4150));
        }
    }

    @Override
    protected Set<HostAndPort> lookupTopic(String topic) {
        return nsqds;
    }

}
