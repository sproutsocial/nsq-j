package com.sproutsocial.nsq;

import org.slf4j.Logger;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.slf4j.LoggerFactory.getLogger;

class ConnectionDetails {
    private static final Logger LOGGER = getLogger(ConnectionDetails.class);
    private final Publisher parent;
    private final BasePubSub basePubSub;
    HostAndPort hostAndPort;
    PubConnection con = null;
    boolean isFailover = false;
    long failoverStart = 0;
    private volatile int failoverDurationSecs;

    public ConnectionDetails(String hostAndPort, Publisher parent, int failoverDurationSecs, BasePubSub basePubSub) {
        this.hostAndPort = HostAndPort.fromString(hostAndPort).withDefaultPort(4150);
        this.parent = parent;
        this.failoverDurationSecs = failoverDurationSecs;
        this.basePubSub = basePubSub;
    }

    /**
     * @return true if this host is ready to receive data
     */
    protected synchronized boolean makeReady() {
        if (con == null) {
            if (parent.isStopping) {
                throw new NSQException("publisher stopped");
            }
            return connectAttempt();
        } else if (isFailover && canAttemptRecovery()) {
            isFailover = false;
            connectAttempt();
            return true;
        }
        return !(isFailover || con == null);
    }

    private boolean canAttemptRecovery() {
        return Util.clock() - failoverStart > TimeUnit.SECONDS.toMillis(failoverDurationSecs);
    }

    private boolean connectAttempt() {
        if (con != null) {
            con.close();
        }
        con = new PubConnection(basePubSub.getClient(), this.hostAndPort, parent);
        try {
            con.connect(basePubSub.getConfig());
        } catch (IOException e) {
            markFailure();
            return false;
        }
        LOGGER.info("publisher connected:{}", hostAndPort);
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConnectionDetails that = (ConnectionDetails) o;
        return hostAndPort.equals(that.hostAndPort);
    }

    @Override
    public int hashCode() {
        return hostAndPort.hashCode();
    }

    protected synchronized void markFailure() {
        Util.closeQuietly(con);
        con = null;
        isFailover = true;
        failoverStart = Util.clock();
        LOGGER.warn("Failed to connect to {}, will retry later", hostAndPort);
    }

    public void setFailoverDurationSecs(int failoverDurationSecs) {
        this.failoverDurationSecs = failoverDurationSecs;
    }

    @Override
    public String toString() {
        return "ConnectionDetails{" +
                "hostAndPort=" + hostAndPort +
                ", con=" + con +
                ", isFailover=" + isFailover +
                ", failoverStart=" + failoverStart +
                ", failoverDurationSecs=" + failoverDurationSecs +
                '}';
    }
}
