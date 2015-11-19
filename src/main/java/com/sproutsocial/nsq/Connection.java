package com.sproutsocial.nsq;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import com.sproutsocial.nsq.jmx.ConnectionMXBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyFramedInputStream;
import org.xerial.snappy.SnappyFramedOutputStream;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Constructor;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

import static com.google.common.base.MoreObjects.firstNonNull;

abstract class Connection extends BasePubSub implements Closeable, ConnectionMXBean {

    protected final HostAndPort host;

    protected DataOutputStream out;
    protected DataInputStream in;
    private volatile boolean isReading = true;

    protected int msgTimeout = 60000;
    protected int heartbeatInterval = 30000;
    protected int maxRdyCount = 2500;

    protected long lastActionFlush; //time the last action was flushed (NOP does not count)
    protected int unflushedCount;
    protected long lastHeartbeat;

    protected BlockingQueue<String> respQueue = new ArrayBlockingQueue<String>(1);

    private static final ThreadFactory readThreadFactory = Util.threadFactory("nsq-read");
    private static final Set<String> nonFatalErrors = ImmutableSet.of("E_FIN_FAILED", "E_REQ_FAILED", "E_TOUCH_FAILED");

    private static final Logger logger = LoggerFactory.getLogger(Connection.class);

    public Connection(HostAndPort host) {
        this.host = host;
    }

    public synchronized void connect(Config config) throws IOException {
        addClientConfig(config);
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
        setConfig(serverConfig);
        msgTimeout = firstNonNull(serverConfig.getMsgTimeout(), 60000);
        heartbeatInterval = firstNonNull(serverConfig.getHeartbeatInterval(), 30000);
        maxRdyCount = firstNonNull(serverConfig.getMaxRdyCount(), 2500);
        logger.info("msgTimeout:{} heartbeatInterval:{} maxRdyCount:{}", msgTimeout, heartbeatInterval, maxRdyCount);

        sock.setSoTimeout(heartbeatInterval + 5000);

        wrapCompression(serverConfig, bufIn, sock);

        scheduleAtFixedRate(new Runnable() {
            public void run() {
                checkHeartbeat();
            }
        }, heartbeatInterval + 2000, heartbeatInterval, false);
        lastHeartbeat = Client.clock();

        readThreadFactory.newThread(new Runnable() {
            public void run() {
                read();
            }
        }).start();
    }

    private void addClientConfig(Config config) {
        if (config.getHostname() == null) {
            String pidHost = ManagementFactory.getRuntimeMXBean().getName();
            int pos = pidHost.indexOf('@');
            if (pos > 0) {
                config.setHostname(pidHost.substring(pos + 1));
            }
        }
        String version = getClass().getPackage().getImplementationVersion();
        logger.debug("version:{}", version);
        if (version == null || version.length() == 0) {
            version = "dev";
        }
        config.setUserAgent("nsq-j/" + version);
        config.setFeatureNegotiation(true);
    }

    private void wrapCompression(ServerConfig serverConfig, BufferedInputStream bufIn, Socket sock) throws IOException {
        if (serverConfig.getDeflate()) {
            in = new DataInputStream(new InflaterInputStream(bufIn,
                    new Inflater(true), 32768));
            //TODO select compression level
            Deflater deflater = new Deflater(Deflater.DEFAULT_COMPRESSION, true);
            try {
                //deflate only works on java7 (syncFlush not exposed on java6), use reflection to see if it is available
                // http://bugs.java.com/bugdatabase/view_bug.do?bug_id=4206909
                Constructor<DeflaterOutputStream> constr = DeflaterOutputStream.class.getConstructor(
                        OutputStream.class, Deflater.class, int.class, boolean.class);
                DeflaterOutputStream deflaterOut = constr.newInstance(sock.getOutputStream(), deflater, 32768, true);
                out = new DataOutputStream(deflaterOut);
            }
            catch (Exception e) {
                throw new NSQException("deflate compression only supported on java7 and up");
            }
            readResponse();
        }
        else if (serverConfig.getSnappy()) {
            in = new DataInputStream(new SnappyFramedInputStream(bufIn));
            out = new DataOutputStream(new SnappyFramedOutputStream(sock.getOutputStream()));
            readResponse();
        }
    }

    protected synchronized void checkHeartbeat() {
        try {
            //logger.debug("checkHeartbeat {}", toString());
            long now = Client.clock();
            if (now - lastHeartbeat > 2 * heartbeatInterval + 2000) {
                logger.info("heartbeat failed, closing connection:{}", toString());
                close();
            }
            else if (now - lastActionFlush > heartbeatInterval * 0.9) {
                //logger.debug("idle, sending NOP {}", toString());
                out.write("NOP\n".getBytes(Charsets.US_ASCII));
                out.flush(); //NOP does not update lastActionFlush
            }
        }
        catch (Exception e) {
            logger.error("problem checking heartbeat, will probably time out soon. con:{}", toString(), e);
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
        String response = null;

        if (frameType == 0) {       //response
            response = readAscii(size - 4);
            if ("_heartbeat_".equals(response)) {
                synchronized (this) {
                    lastHeartbeat = Client.clock();
                }
                response = null;
            }
        }
        else if (frameType == 1) {  //error
            String error = readAscii(size - 4);
            if (nonFatalErrors.contains(error)) {
                logger.warn("non fatal nsqd error:{} probably due to message timeout", error);
            }
            else {
                throw new NSQException("error from nsqd:" + error);
            }
        }
        else if (frameType == 2) {  //message
            onMessage(in.readLong(), in.readUnsignedShort(), readAscii(16), readBytes(size - 30));
        }
        else {
            throw new NSQException("bad frame type:" + frameType);
        }
        return response;
    }

    private void read() {
        try {
            while (isReading) {
                //no need to synchronize, this is the only thread that reads after connect()
                String response = readResponse();
                if (response != null) {
                    respQueue.offer(response);
                }
            }
        }
        catch (Exception e) {
            if (isReading) {
                close();
                logger.error("read thread exception. con:{}", toString(), e);
            }
        }
        logger.debug("read loop done");
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

    public synchronized void flushAndClose() {
        try {
            flush();
        }
        catch (IOException e) {
            logger.error("flush on close error", e);
        }
        close();
    }

    public synchronized void close() {
        isReading = false;
        Util.closeQuietly(out);
        Util.closeQuietly(in);
        Client.eventBus.post(this);
        logger.debug("connection closed:{}", toString());
    }

    public HostAndPort getHost() {
        return host;
    }

    @Override
    public synchronized String toString() {
        long now = Client.clock();
        return String.format("%s lastFlush:%.1f lastHeartbeat:%.1f unflushedCount:%d", host.getHostText(),
                (now - lastActionFlush) / 1000f, (now - lastHeartbeat) / 1000f, unflushedCount);
    }

    @Override
    public synchronized int getMsgTimeout() {
        return msgTimeout;
    }

    @Override
    public synchronized long getLastActionFlush() {
        return lastActionFlush;
    }

    @Override
    public synchronized int getMaxRdyCount() {
        return maxRdyCount;
    }

    @Override
    public synchronized int getHeartbeatInterval() {
        return heartbeatInterval;
    }

    @Override
    public synchronized int getUnflushedCount() {
        return unflushedCount;
    }

    @Override
    public synchronized long getLastHeartbeat() {
        return lastHeartbeat;
    }

}
