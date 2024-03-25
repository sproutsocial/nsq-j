package com.sproutsocial.nsq;

import net.jcip.annotations.GuardedBy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLSocketFactory;
import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

import static com.sproutsocial.nsq.Util.firstNonNull;

abstract class Connection extends BasePubSub implements Closeable {

    protected final HostAndPort host;

    protected DataOutputStream out;
    protected DataInputStream in;
    protected volatile boolean isReading = true;

    protected int msgTimeout = 60000;
    protected int heartbeatInterval = 30000;
    protected int maxRdyCount = 2500;

    protected long lastActionFlush; //time the last action was flushed (NOP does not count)
    protected int unflushedCount;
    protected long lastHeartbeat;

    protected final BlockingQueue<String> respQueue = new ArrayBlockingQueue<String>(1);
    protected final ExecutorService handlerExecutor;

    private static final ThreadFactory readThreadFactory = Util.threadFactory("nsq-read");
    private static final Set<String> nonFatalErrors = Collections.unmodifiableSet(new HashSet<String>(
            Arrays.asList("E_FIN_FAILED", "E_REQ_FAILED", "E_TOUCH_FAILED")));

    private static final Logger logger = LoggerFactory.getLogger(Connection.class);

    public Connection(Client client, HostAndPort host) {
        super(client);
        this.host = host;
        this.handlerExecutor = client.getExecutor();
    }

    public synchronized void connect(Config config) throws IOException {
        addClientConfig(config);
        Socket sock = new Socket();
        sock.setSoTimeout(30000);
        sock.connect(new InetSocketAddress(host.getHost(), host.getPort()), 30000);
        StreamPair streams = setStreams(sock.getInputStream(), sock.getOutputStream(), new StreamPair());
        out.write("  V2".getBytes(Util.US_ASCII));

        String response = connectCommand("IDENTIFY", client.getGson().toJson(config).getBytes(Util.UTF_8));
        ServerConfig serverConfig = client.getGson().fromJson(response, ServerConfig.class);
        logger.debug("serverConfig:{}", response);
        setConfig(serverConfig);
        msgTimeout = firstNonNull(serverConfig.getMsgTimeout(), 60000);
        heartbeatInterval = firstNonNull(serverConfig.getHeartbeatInterval(), 30000);
        maxRdyCount = firstNonNull(serverConfig.getMaxRdyCount(), 2500);
        logger.info("connected {} msgTimeout:{} heartbeatInterval:{} maxRdyCount:{}", host, msgTimeout, heartbeatInterval, maxRdyCount);

        sock.setSoTimeout(heartbeatInterval + 5000);

        wrapEncryption(serverConfig, sock, streams);
        wrapCompression(serverConfig, streams);

        if (!streams.isBuffered) {
            in = new DataInputStream(new BufferedInputStream(streams.baseIn));
            out = new DataOutputStream(new BufferedOutputStream(streams.baseOut));
        }

        sendAuthorization(serverConfig);

        scheduleAtFixedRate(new Runnable() {
            public void run() {
                checkHeartbeat();
            }
        }, heartbeatInterval + 2000, heartbeatInterval, false);
        lastHeartbeat = Util.clock();

        readThreadFactory.newThread(new Runnable() {
            public void run() {
                read();
            }
        }).start();
    }

    private String connectCommand(String command, byte[] data) throws IOException {
        out.write((command + "\n").getBytes(Util.US_ASCII));
        write(data);
        out.flush();
        return readResponse();
    }

    private void addClientConfig(Config config) {
        if (config.getHostname() == null) {
            String pidHost = ManagementFactory.getRuntimeMXBean().getName();
            int pos = pidHost.indexOf('@');
            if (pos > 0) {
                config.setHostname(pidHost.substring(pos + 1));
            }
        }
        config.setFeatureNegotiation(true);
    }

    private void wrapEncryption(ServerConfig serverConfig, Socket baseSocket, StreamPair streams) throws IOException {
        if (!serverConfig.getTlsV1()) {
            return;
        }
        logger.debug("adding tls");
        SSLSocketFactory sockFactory = client.getSSLSocketFactory();
        if (sockFactory == null) {
            sockFactory = (SSLSocketFactory) SSLSocketFactory.getDefault();
        }
        Socket sslSocket = sockFactory.createSocket(baseSocket, baseSocket.getInetAddress().getHostAddress(), baseSocket.getPort(), true);
        setStreams(sslSocket.getInputStream(), sslSocket.getOutputStream(), streams);
        readResponse();
    }

    private void wrapCompression(ServerConfig serverConfig, StreamPair streams) throws IOException {
        if (serverConfig.getDeflate()) {
            logger.debug("adding deflate compression");
            try {
                InflaterInputStream inflateIn = new InflaterInputStream(streams.baseIn, new Inflater(true), 32768);
                Deflater deflater = new Deflater(Deflater.DEFAULT_COMPRESSION, true);
                //deflate only works on java7 (syncFlush not exposed on java6), use reflection to see if it is available
                // http://bugs.java.com/bugdatabase/view_bug.do?bug_id=4206909
                Constructor<DeflaterOutputStream> constr = DeflaterOutputStream.class.getConstructor(
                        OutputStream.class, Deflater.class, int.class, boolean.class);
                DeflaterOutputStream deflateOut = constr.newInstance(streams.baseOut, deflater, 32768, true);

                setStreams(inflateIn, deflateOut, streams);
                streams.isBuffered = true;
                readResponse();
            }
            catch (Exception e) {
                throw new NSQException("deflate compression only supported on java7 and up");
            }
        }
        else if (serverConfig.getSnappy()) {
            logger.debug("adding snappy compression");
            if (serverConfig.getVersion().startsWith("0.")) {
                throw new NSQException("snappy compression only supported on nsqd 1.0 and up");
            }
            try {
                //hacky, use reflection to keep snappy dependency optional, it is finicky about versions
                Constructor snappyInConstr = Class.forName("org.xerial.snappy.SnappyFramedInputStream").getConstructor(InputStream.class);
                Constructor snappyOutConstr = Class.forName("org.xerial.snappy.SnappyFramedOutputStream").getConstructor(OutputStream.class);
                setStreams((InputStream) snappyInConstr.newInstance(streams.baseIn),
                        (OutputStream) snappyOutConstr.newInstance(streams.baseOut), streams);
                //streams.isBuffered = true; //not sure if additional buffers help or hurt, try using them for now
                readResponse();
            }
            catch (Exception e) {
                throw new NSQException("snappy compression failed, is org.xerial.snappy:snappy-java available?", e);
            }
        }
    }

    private void sendAuthorization(ServerConfig serverConfig) throws IOException {
        if (serverConfig.getAuthRequired() != null && serverConfig.getAuthRequired()) {
            if (client.getAuthSecret() == null) {
                throw new NSQException("nsqd requires authorization, call client.setAuthSecret before connecting");
            }
            if (!serverConfig.getTlsV1()) {
                logger.warn("authorization used without encryption. authSecret sent in plain text");
            }
            String authResponse = connectCommand("AUTH", client.getAuthSecret());
            logger.info("authorization response:{}", authResponse);
            //no need to check response, future PUB/SUB may fail with E_UNAUTHORIZED
        }
    }

    private void checkHeartbeat() {
        try {
            boolean isDead = true;
            long now = Util.clock();
            synchronized (this) {
                isDead = now - lastHeartbeat > 2 * heartbeatInterval;
            }
            if (isDead) {
                logger.info("heartbeat failed, closing connection:{}", toString());
                close();
            }
        }
        catch (Exception e) {
            logger.error("problem checking heartbeat, will probably time out soon. {}", toString(), e);
        }
    }

    @GuardedBy("this")
    protected void writeCommand(String cmd, Object param1, Object param2) throws IOException {
        out.write((cmd + " " + param1 + " " + param2 + "\n").getBytes(Util.US_ASCII));
    }

    @GuardedBy("this")
    protected void writeCommand(String cmd, Object param) throws IOException {
        out.write((cmd + " " +  param + "\n").getBytes(Util.US_ASCII));
    }

    @GuardedBy("this")
    protected void writeCommand(String cmd) throws IOException {
        out.write((cmd + "\n").getBytes(Util.US_ASCII));
    }

    @GuardedBy("this")
    protected void write(byte[] data) throws IOException {
        out.writeInt(data.length);
        out.write(data);
    }

    @GuardedBy("this")
    protected void flush() throws IOException {
        out.flush();
        lastActionFlush = Util.clock();
        unflushedCount = 0;
    }

    private String readResponse() throws IOException {
        int size = in.readInt();
        int frameType = in.readInt();
        String response = null;

        if (frameType == 0) {       //response
            response = readAscii(size - 4);
        }
        else if (frameType == 1) {  //error
            String error = readAscii(size - 4);
            int index = error.indexOf(" ");
            String errorCode = index == -1 ? error : error.substring(0, index);
            if (nonFatalErrors.contains(errorCode)) {
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
                if ("_heartbeat_".equals(response)) {
                    //don't block this thread
                    client.getSchedExecutor().execute(new Runnable() {
                        public void run() {
                            receivedHeartbeat();
                        }
                    });
                }
                else if (response != null) {
                    respQueue.offer(response);
                }
            }
        }
        catch (EOFException e) {
            if (isReading) {
                logger.info("read thread closed connection. con:{}", toString());
                close();
            }
        }
        catch (Exception e) {
            if (isReading) {
                respQueue.offer(e.toString());
                close();
                logger.error("read thread exception. con:{}", toString(), e);
            }
        }
        logger.debug("read loop done {}", toString());
    }

    private synchronized void receivedHeartbeat() {
        try {
            out.write("NOP\n".getBytes(Util.US_ASCII));
            out.flush(); //NOP does not update lastActionFlush
            lastHeartbeat = Util.clock();
        }
        catch (Throwable t) {
            logger.error("receivedHeartbeat error", t);
        }
    }

    protected void onMessage(long timestamp, int attempts, String id, byte[] data) {
        throw new NSQException("unexpected frame type 2 - message"); //overridden by SubConnection
    }

    private byte[] readBytes(int size) throws IOException {
        byte[] data = new byte[size];
        in.readFully(data);
        return data;
    }

    private String readAscii(int size) throws IOException {
        return new String(readBytes(size), Util.US_ASCII);
    }

    protected void flushAndReadOK() throws IOException {
        flushAndReadResponse("OK");
    }

    protected void flushAndReadResponse(final String expectedResponse) throws IOException {
        flush();
        try {
            String resp = respQueue.poll(heartbeatInterval, TimeUnit.MILLISECONDS);
            if (!expectedResponse.equals(resp)) {
                throw new NSQException("bad response:" + (resp != null ? resp : "timeout"));
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new NSQException("read interrupted");
        }
    }

    public synchronized void flushAndClose() {
        try {
            flush();
        }
        catch (IOException e) {
            logger.error("flushAndClose error", e);
        }
        close();
    }

    public void close() {
        isReading = false;
        Util.closeQuietly(out);
        Util.closeQuietly(in);
        cancelTasks();
        logger.debug("connection closed:{}", toString());
    }

    public HostAndPort getHost() {
        return host;
    }

    public synchronized String stateDesc() {
        long now = Util.clock();
        return String.format("%s lastFlush:%.1f lastHeartbeat:%.1f unflushedCount:%d", toString(),
                (now - lastActionFlush) / 1000f, (now - lastHeartbeat) / 1000f, unflushedCount);
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

    //helpers used during initialization only
    private static class StreamPair {
        private InputStream baseIn;
        private OutputStream baseOut;
        boolean isBuffered = false;
    }

    private StreamPair setStreams(InputStream baseIn, OutputStream baseOut, StreamPair streams) {
        streams.baseIn = baseIn;
        streams.baseOut = baseOut;
        in = new DataInputStream(baseIn);
        out = new DataOutputStream(baseOut);
        return streams;
    }

}
