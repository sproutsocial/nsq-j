package com.sproutsocial.nsq;

import com.google.common.eventbus.Subscribe;
import com.google.common.net.HostAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class Publisher extends Client {

    private final HostAndPort nsqd;
    private final HostAndPort backupNsqd;
    private PubConnection con;
    private boolean isBackupConnected = false;
    private long blackListStart;

    private static final int blackListDuration = 30000;

    private static final Logger logger = LoggerFactory.getLogger(Publisher.class);

    public Publisher(String nsqd, String backupNsqd) {
        this.nsqd = HostAndPort.fromString(nsqd).withDefaultPort(4150);
        this.backupNsqd = backupNsqd != null ? HostAndPort.fromString(backupNsqd).withDefaultPort(4150) : null;
        Client.eventBus.register(this);
    }

    public Publisher(String nsqd) {
        this(nsqd, null);
    }

    private void checkConnection() throws IOException {
        if (con == null) {
            connect(nsqd);
        }
        else if (isBackupConnected && Client.clock() - blackListStart > blackListDuration) {
            connect(nsqd);
            isBackupConnected = false;
            logger.info("using primary nsqd");
        }
    }

    private void connect(HostAndPort host) throws IOException {
        if (con != null) {
            con.close();
        }
        con = new PubConnection(host, this);
        con.connect(config);
        logger.info("publisher connected:{}", host);
    }

    @Subscribe
    public synchronized void connectionClosed(Connection closedCon) {
        if (con == closedCon) {
            con = null;
            logger.debug("removed closed connection:{}", closedCon.getHost());
        }
    }

    public synchronized void publish(String topic, byte[] data) {
        try {
            checkConnection();
            con.publish(topic, data);
        }
        catch (IOException e) {
            publishBackup(topic, data);
        }
    }

    public synchronized void publish(String topic, List<byte[]> dataList) {
        try {
            checkConnection();
            con.publish(topic, dataList);
        }
        catch (IOException e) {
            for (byte[] data : dataList) {
                publishBackup(topic, data);
            }
        }
    }

    private void publishBackup(String topic, byte[] data) {
        try {
            if (backupNsqd == null) {
                logger.info("publish failed, will sleep and retry once");
                Util.sleepQuietly(10000);
                connect(nsqd);
            }
            else if (!isBackupConnected) {
                blackListStart = Client.clock();
                isBackupConnected = true;
                connect(backupNsqd);
                logger.info("using backup nsqd");
            }
            con.publish(topic, data);
        }
        catch (IOException e) {
            throw new NSQException("publish failed", e);
        }
    }

}
