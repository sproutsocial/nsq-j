package com.sproutsocial.nsq;

import com.google.common.net.HostAndPort;

import java.io.IOException;
import java.util.List;

class PubConnection extends Connection {

    private final Publisher publisher;

    public PubConnection(Client client, HostAndPort host, Publisher publisher) {
        super(client, host);
        this.publisher = publisher;
    }

    public synchronized void publish(String topic, byte[] data) throws IOException {
        respQueue.clear();
        writeCommand("PUB", topic);
        write(data);
        flushAndReadOK();
    }

    public synchronized void publish(String topic, List<byte[]> dataList) throws IOException {
        respQueue.clear();
        writeCommand("MPUB", topic);
        int bodySize = 4;
        for (byte[] data : dataList) {
            bodySize += data.length + 4;
        }
        out.writeInt(bodySize);
        out.writeInt(dataList.size());
        for (byte[] data : dataList) {
            write(data);
        }
        flushAndReadOK();
    }

    @Override
    public void close() {
        super.close();
        if (!publisher.isStopping) {
            //be paranoid about locks, we only care that this happens sometime soon
            executor.execute(new Runnable() {
                public void run() {
                    publisher.connectionClosed(PubConnection.this);
                }
            });
        }
    }

    @Override
    public String toString() {
        return String.format("PubCon:%s", host.getHost());
    }

}
