package com.sproutsocial.nsq;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.sproutsocial.nsq.Util.checkArgument;
import static com.sproutsocial.nsq.Util.checkNotNull;

@ThreadSafe
public class RoundRobinPublisher extends BasePubSub implements PublisherInterface {
    private Publisher parent;

    private class ConnectionDetails {
        HostAndPort hostAndPort;
        PubConnection con = null;
        boolean isFailover = false;
        long failoverStart = 0;

        public ConnectionDetails(String hostAndPort) {
            this.hostAndPort = HostAndPort.fromString(hostAndPort).withDefaultPort(4150);
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
                logger.info("using primary nsqd");
                return true;
            }
            return !(isFailover || con == null);
        }

        private boolean canAttemptRecovery() {
            return Util.clock() - failoverStart > failoverDurationSecs * 1000;
        }

        private boolean connectAttempt() {
            if (con != null) {
                con.close();
            }
            con = new PubConnection(client, this.hostAndPort, parent);
            try {
                con.connect(config);
            } catch (IOException e) {
                con.close();
                con = null;
                isFailover = true;
                failoverStart = Util.clock();
                logger.warn("Failed to connect to {}, will retry later", hostAndPort);
                return false;
            }
            logger.info("publisher connected:{}", hostAndPort);
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


        @Override
        public String toString() {
            return "ConnectionDetails{" +
                    "hostAndPort=" + hostAndPort +
                    '}';
        }

        protected synchronized void markFailure() {
            con = null;
            isFailover = true;
            failoverStart = Util.clock();
            logger.warn("Failed to connect to {}, will retry later", hostAndPort);
        }

        public void close() {
            Util.closeQuietly(con);
            con = null;
        }
    }

    private final List<ConnectionDetails> daemonList;
    private int nextDaemonIndex = 0;
    private int failoverDurationSecs = 300;

    private static final Logger logger = LoggerFactory.getLogger(RoundRobinPublisher.class);

    public RoundRobinPublisher(Client client, String nsqd, String failoverNsqd, Publisher parent) {
        super(client);
        this.parent = parent;
        Set<ConnectionDetails> connectionDetails = new HashSet<ConnectionDetails>();
        daemonList = Collections.unmodifiableList(getConnectionDetails(nsqd, failoverNsqd, connectionDetails));
    }

    private List<ConnectionDetails> getConnectionDetails(String nsqd, String failoverNsqd, Set<ConnectionDetails> connectionDetails) {
        for (String host : nsqd.split(",")) {
            connectionDetails.add(new ConnectionDetails(host));
        }
        if (failoverNsqd != null) {
            for (String host : failoverNsqd.split(",")) {
                connectionDetails.add(new ConnectionDetails(host));
            }
        }
        return new ArrayList<ConnectionDetails>(connectionDetails);
    }

    @Override
    public synchronized void connectionClosed(PubConnection closedCon) {
        for (ConnectionDetails daemon : daemonList) {
            if (daemon.con == closedCon) {
                daemon.con = null;
                logger.debug("removed closed publisher connection:{}", closedCon.getHost());
            }
        }
    }

    @Override
    public synchronized void publish(String topic, byte[] data) {
        checkNotNull(topic);
        checkNotNull(data);
        checkArgument(data.length > 0);
        ConnectionDetails nextDaemon = getNextDaemon();
        try {
            nextDaemon.con.publish(topic, data);
        } catch (Exception e) {
            nextDaemon.markFailure();
            logger.error("publish error with:{}", nextDaemon, e);
            publish(topic, data);//Recurse to get the next daemon and try again
        }
    }

    @GuardedBy("this")
    private ConnectionDetails getNextDaemon() {
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

    @Override
    public synchronized void publishDeferred(String topic, byte[] data, long delay, TimeUnit unit) {
        checkNotNull(topic);
        checkNotNull(data);
        checkArgument(data.length > 0);
        checkArgument(delay > 0);
        checkNotNull(unit);
        ConnectionDetails nextDaemon = getNextDaemon();
        try {
            nextDaemon.con.publishDeferred(topic, data, unit.toMillis(delay));
        } catch (Exception e) {
            nextDaemon.markFailure();
            //deferred publish never fails over
            throw new NSQException("deferred publish failed", e);
        }
    }

    @Override
    public synchronized void publish(String topic, List<byte[]> dataList) {
        checkNotNull(topic);
        checkNotNull(dataList);
        checkArgument(dataList.size() > 0);
        ConnectionDetails nextDaemon = getNextDaemon();
        try {
            nextDaemon.con.publish(topic, dataList);
        } catch (Exception e) {
            nextDaemon.markFailure();
            logger.error("publish error with:{}", nextDaemon, e);
            publish(topic, dataList);//Recurse to get next
        }
    }


    @Override
    public synchronized void stop() {
        super.stop();
        for (ConnectionDetails daemon : daemonList) {
            daemon.close();
        }
    }

    @Override
    public synchronized int getFailoverDurationSecs() {
        return failoverDurationSecs;
    }

    @Override
    public synchronized void setFailoverDurationSecs(int failoverDurationSecs) {
        this.failoverDurationSecs = failoverDurationSecs;
    }

}
