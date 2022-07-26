package com.sproutsocial.nsq;

import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;

import java.util.Arrays;

import static org.slf4j.LoggerFactory.getLogger;

@ThreadSafe
public class FailoverBalenceStrategy extends ListBasedBallenceStrategy implements BalanceStrategy {

    public FailoverBalenceStrategy(Client client, String nsqd, String failoverNsqd, Publisher parent) {
        super(client, parent, Arrays.asList(nsqd, failoverNsqd));
    }

    @Override
    public ConnectionDetails getCurrentConnectionDetails() {
        for (int attempts = 0; attempts < daemonList.size(); attempts++) {
            ConnectionDetails candidate = daemonList.get(attempts);
            if (candidate.makeReady()) {
                return candidate;
            }
        }
        throw new NSQException("publish failed: Unable to establish a connection with any NSQ host: " + daemonList);
    }


}
