package com.sproutsocial.nsq;

import org.slf4j.Logger;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.slf4j.LoggerFactory.getLogger;
import static com.sproutsocial.nsq.Util.*;

class ConnectionDetails {
    private enum State {
        CONNECTED,
        NOT_CONNECTED,
        FAILED
    }

    private static final Logger LOGGER = getLogger(ConnectionDetails.class);
    private final Publisher parent;
    private final BasePubSub basePubSub;
    HostAndPort hostAndPort;
    private PubConnection con = null;
    long failoverStart = 0;
    private volatile int failoverDurationSecs;
    private State currentState = State.NOT_CONNECTED;

    public ConnectionDetails(String hostAndPort, Publisher parent, int failoverDurationSecs, BasePubSub basePubSub) {
        checkNotNull(hostAndPort);
        checkNotNull(parent);
        checkNotNull(basePubSub);
        this.hostAndPort = HostAndPort.fromString(hostAndPort).withDefaultPort(4150);
        this.parent = parent;
        this.failoverDurationSecs = failoverDurationSecs;
        this.basePubSub = basePubSub;
    }

    /**
     * @return true if this host is ready to receive data
     */
    protected synchronized boolean makeReady() {
        if (currentState == State.NOT_CONNECTED) {
            if (parent.isStopping) {
                throw new NSQException("publisher stopped");
            }
            return connectAttempt();
        } else if (currentState == State.FAILED && canAttemptRecovery()) {
            return connectAttempt();
        }
        return currentState == State.CONNECTED;
    }

    private boolean canAttemptRecovery() {
        return Util.clock() - failoverStart > TimeUnit.SECONDS.toMillis(failoverDurationSecs);
    }

    private boolean connectAttempt() {
        if (getCon() != null) {
            getCon().close();
        }
        setCon(new PubConnection(basePubSub.getClient(), this.hostAndPort, parent));
        try {
            getCon().connect(basePubSub.getConfig());
            currentState = State.CONNECTED;
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

    public synchronized void markFailure() {
        Util.closeQuietly(getCon());
        setCon(null);
        currentState = State.FAILED;
        failoverStart = Util.clock();
        LOGGER.warn("Failed to connect to {}, will retry later", hostAndPort);
    }

    public void setFailoverDurationSecs(int failoverDurationSecs) {
        this.failoverDurationSecs = failoverDurationSecs;
    }


    public PubConnection getCon() {
        return con;
    }

    private void setCon(PubConnection con) {
        this.con = con;
    }

    public void clearConnection() {
        this.con = null;
        currentState = State.NOT_CONNECTED;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ConnectionDetails{");
        sb.append("parent=").append(parent);
        sb.append(", basePubSub=").append(basePubSub);
        sb.append(", hostAndPort=").append(hostAndPort);
        sb.append(", con=").append(con);
        sb.append(", failoverStart=").append(failoverStart);
        sb.append(", failoverDurationSecs=").append(failoverDurationSecs);
        sb.append(", currentState=").append(currentState);
        sb.append('}');
        return sb.toString();
    }
}