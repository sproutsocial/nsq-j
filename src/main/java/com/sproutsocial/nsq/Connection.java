package com.sproutsocial.nsq;

import com.google.common.base.Charsets;
import com.google.common.net.HostAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyFramedInputStream;
import org.xerial.snappy.SnappyFramedOutputStream;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.zip.*;

import static com.google.common.base.MoreObjects.firstNonNull;

abstract class Connection {

    protected final HostAndPort host;

    protected DataOutputStream out;
    protected DataInputStream in;

    protected int msgTimeout = 60000;
    protected int heartbeatInterval = 30000;
    protected int maxRdyCount = 2500;

    protected long lastActionFlush; //time the last action was flushed (NOP does not count)
    protected int unflushedCount;
    protected long lastHeartbeat;

    private volatile boolean isRunning = true;
    protected BlockingQueue<String> respQueue = new ArrayBlockingQueue<String>(1);
    protected List<ScheduledFuture> tasks = new ArrayList<ScheduledFuture>();

    private static ThreadFactory readThreadFactory = Util.threadFactory("nsq-read");

    private static final Logger logger = LoggerFactory.getLogger(Connection.class);

    public Connection(HostAndPort host) {
        this.host = host;
    }

    public synchronized void connect(Config config) throws IOException {
        Socket sock = new Socket(host.getHostText(), host.getPort());
        BufferedInputStream bufIn = new BufferedInputStream(sock.getInputStream());
        in = new DataInputStream(bufIn);
        out = new DataOutputStream(new BufferedOutputStream(sock.getOutputStream()));
        out.write("  V2".getBytes());

        out.write("IDENTIFY\n".getBytes());
        write(Client.mapper.writeValueAsBytes(config));
        out.flush();
        String response = readResponse();

        ServerConfig serverConfig = Client.mapper.readValue(response, ServerConfig.class);
        logger.debug("serverConfig:{}", Client.mapper.writerWithDefaultPrettyPrinter().writeValueAsString(serverConfig));
        msgTimeout = firstNonNull(serverConfig.getMsgTimeout(), 60000);
        heartbeatInterval = firstNonNull(serverConfig.getHeartbeatInterval(), msgTimeout / 2);
        maxRdyCount = firstNonNull(serverConfig.getMaxRdyCount(), 2500);

        sock.setSoTimeout(heartbeatInterval + 5000);

        if (serverConfig.getDeflate()) {
            in = new DataInputStream(new InflaterInputStream(bufIn,
                    new Inflater(true), 32768));
            //java7 - works
            //out = new DataOutputStream(new DeflaterOutputStream(sock.getOutputStream(),
            //        new Deflater(Deflater.DEFAULT_COMPRESSION, true), 32768, true));
            //java6 - does not work, no way to turn on syncFlush
            out = new DataOutputStream(new DeflaterOutputStream(sock.getOutputStream(),
                    new Deflater(Deflater.DEFAULT_COMPRESSION, true), 32768));
            response = readResponse();
        }
        else if (serverConfig.getSnappy()) {
            in = new DataInputStream(new SnappyFramedInputStream(bufIn));
            out = new DataOutputStream(new SnappyFramedOutputStream(sock.getOutputStream()));
            response = readResponse();
        }

        scheduleAtFixedRate(new Runnable() {
            public void run() {
                checkHeartbeat();
            }
        }, heartbeatInterval + 2000, heartbeatInterval);
        lastHeartbeat = Client.clock();

        readThreadFactory.newThread(new Runnable() {
            public void run() {
                read();
            }
        }).start();
    }

    protected synchronized void checkHeartbeat() {
        try {
            logger.debug("checkHeartbeat");
            long now = Client.clock();
            if (now - lastHeartbeat > 2 * heartbeatInterval + 2000) {
                logger.info("heartbeat failed, closing connection:{}", host);
                close();
            }
            else if (now - lastActionFlush > heartbeatInterval * 0.9) {
                logger.debug("idle, sending NOP");
                out.write("NOP\n".getBytes(Charsets.US_ASCII));
                out.flush(); //NOP does not update lastActionFlush
            }
        }
        catch (Exception e) {
            logger.error("problem checking heartbeat, will probably time out soon", e);
        }
    }

    protected void writeCommand(String cmd, Object param1, Object param2) throws IOException {
        out.write(String.format("%s %s %s\n", cmd, param1, param2).getBytes(Charsets.US_ASCII));
    }

    protected void writeCommand(String cmd, Object param) throws IOException {
        out.write(String.format("%s %s\n", cmd, param).getBytes(Charsets.US_ASCII));
    }

    protected void write(byte[] data) throws IOException {
        out.writeInt(data.length);
        out.write(data);
    }

    protected void flush() throws IOException {
        out.flush();
        lastActionFlush = Client.clock();
        unflushedCount = 0;
    }

    protected String readResponse() throws IOException {
        int size = in.readInt();
        int frameType = in.readInt();
        logger.debug("reading frame size:{} type:{}", size, frameType);
        String response = null;

        if (frameType == 0) {
            response = readAscii(size - 4);
            if ("_heartbeat_".equals(response)) {
                synchronized (this) {
                    lastHeartbeat = Client.clock();
                }
                response = null;
            }
        }
        else if (frameType == 1) {
            String error = readAscii(size - 4);
            throw new NSQException("error from nsqd:" + error);
        }
        else if (frameType == 2) {
            onMessage(in.readLong(), in.readUnsignedShort(), readAscii(16), readBytes(size - 30));
        }
        else {
            throw new NSQException("bad frame type:" + frameType);
        }
        logger.debug("response:{}", response);
        return response;
    }

    private void read() {
        try {
            while (isRunning) {
                //no need to synchronize, this is the only thread that reads after connect()
                String response = readResponse();
                if (response != null) {
                    respQueue.offer(response);
                }
            }
        }
        catch (Exception e) {
            if (isRunning) {
                close();
                logger.error("read thread exception:{}", e);
            }
        }
    }

    protected void onMessage(long timestamp, int attempts, String id, byte[] data) {
        throw new NSQException("unexpected frame type 2 - message");
    }

    private byte[] readBytes(int size) throws IOException {
        byte[] data = new byte[size];
        in.readFully(data);
        return data;
    }

    private String readAscii(int size) throws IOException {
        return new String(readBytes(size), Charsets.US_ASCII);
    }

    protected void flushAndReadOK() throws IOException {
        flush();
        try {
            String resp = respQueue.poll(msgTimeout, TimeUnit.MILLISECONDS);
            if (!"OK".equals(resp)) {
                throw new NSQException("bad response:" + resp);
            }
        }
        catch (InterruptedException e) {
            throw new NSQException("read interrupted");
        }
    }

    public synchronized void close() {
        isRunning = false;
        for (ScheduledFuture task : tasks) {
            task.cancel(false);
        }
        tasks.clear();
        Util.closeQuietly(out);
        Util.closeQuietly(in);
        Client.eventBus.post(this);
        logger.debug("connection closed");
    }

    public void scheduleAtFixedRate(Runnable runnable, int initialDelay, int period) {
        tasks.add(Client.scheduleAtFixedRate(runnable, initialDelay, period));
    }

    public HostAndPort getHost() {
        return host;
    }

    public synchronized int getMsgTimeout() {
        return msgTimeout;
    }

    public synchronized long getLastActionFlush() {
        return lastActionFlush;
    }

    public synchronized int getMaxRdyCount() {
        return maxRdyCount;
    }

}
