package com.sproutsocial.nsq;

import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.slf4j.LoggerFactory.getLogger;

@ThreadSafe
public class RoundRobbinBallenceStrategy extends ListBasedBallenceStrategy implements BalanceStrategy {
    private int nextDaemonIndex = 0;

    public RoundRobbinBallenceStrategy(Client client, Publisher parent, String nsqd, String failoverNsqd) {
        super(client, parent, getHostNames(nsqd, failoverNsqd));
    }

    private static List<String> getHostNames(String nsqd, String failoverNsqd) {
        List<String> out = new ArrayList<>();
        out.addAll(Arrays.asList(nsqd.split(",")));
        out.addAll(Arrays.asList(failoverNsqd.split(",")));
        return out;
    }

    @Override
    public ConnectionDetails getCurrentConnectionDetails() {
        for (int attempts = 0; attempts < daemonList.size(); attempts++) {
            ConnectionDetails candidate = daemonList.get(nextDaemonIndex);
            if (candidate.makeReady()) {
                return candidate;
            }
            nextDaemonIndex++;
            if (nextDaemonIndex >= daemonList.size()) {
                nextDaemonIndex = 0;
            }
        }
        throw new NSQException("publish failed: Unable to establish a connection with any NSQ host: " + daemonList);
    }


}
