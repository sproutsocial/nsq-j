package com.sproutsocial.nsq;

public interface BalanceStrategy {

    ConnectionDetails getConnectionDetails();

    void connectionClosed(PubConnection closedCon);

    int getFailoverDurationSecs();

    void setFailoverDurationSecs(int failoverDurationSecs);
}
