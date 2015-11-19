package com.sproutsocial.nsq;

import com.google.common.net.HostAndPort;

import java.io.IOException;
import java.util.List;

class PubConnection extends Connection {

    public PubConnection(HostAndPort host) {
        super(host);
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
    public synchronized String toString() {
        return super.toString() + " pub";
    }

}
