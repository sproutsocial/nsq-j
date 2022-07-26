package com.sproutsocial.nsq;

import static com.sproutsocial.nsq.Util.checkNotNull;

public interface BalanceStrategy {
    static BalanceStrategy build(String nsqd, String failoverNsqd, Publisher parent, Client client) {
        checkNotNull(nsqd);
        checkNotNull(parent);
        checkNotNull(client);
        if (nsqd.contains(",")) {
            return new RoundRobbinBallenceStrategy(client, parent, nsqd, failoverNsqd);
        } else if (failoverNsqd == null) {
            return new SingleNsqdBallenceStrategy(client, parent, nsqd);
        } else {
            return new FailoverBalenceStrategy(client, nsqd, failoverNsqd, parent);
        }
    }

    PubConnection getConnection();

    void lastPublishFailed();

    void connectionClosed(PubConnection closedCon);

    int getFailoverDurationSecs();

    void setFailoverDurationSecs(int failoverDurationSecs);
}
