package com.sproutsocial.nsq;

import com.google.common.collect.Maps;
import com.google.common.net.HostAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;

class SubConnection extends Connection {

    private final MessageHandler handler;
    private final ExecutorService executor;
    private final Subscription subscription;
    private final Map<String, Long> inFlight = Maps.newHashMap();
    private int maxInFlight = 1;
    private int maxUnflushed = maxInFlight / 3;
    private int rdy = 0;

    private static final Logger logger = LoggerFactory.getLogger(SubConnection.class);

    public SubConnection(HostAndPort host, Subscription subscription) {
        super(host);
        Subscriber subscriber = subscription.getSubscriber();
        this.handler = subscription.getHandler();
        this.executor = subscriber.getExecutor();
        this.subscription = subscription;

        scheduleAtFixedRate(new Runnable() {
            public void run() {
                delayedFlush();
            }
        }, subscriber.getMaxFlushDelayMillis(), subscriber.getMaxFlushDelayMillis());
        int checkInterval = Math.min(120000, Math.max(20000, (int) (msgTimeout * 0.9)));
        scheduleAtFixedRate(new Runnable() {
            public void run() {
                checkMsgTimeout();
            }
        }, checkInterval, checkInterval);
    }

    public synchronized void finish(String id) throws IOException {
        writeCommand("FIN", id);
        inFlight.remove(id);
        checkReady();
        checkFlush();
    }

    public synchronized void requeue(String id) throws IOException {
        writeCommand("REQ", id, 0);
        inFlight.remove(id);
        checkReady();
        checkFlush();
    }

    public synchronized void touch(String id) throws IOException {
        writeCommand("TOUCH", id);
        inFlight.put(id, Client.clock());
        checkFlush();
    }

    private synchronized void delayedFlush() {
        try {
            if (unflushedCount > 0) {
                logger.debug("delayed flush hit");
                flush();
            }
        }
        catch (Exception e) {
            logger.error("delayedFlush error", e);
        }
    }

    private synchronized void checkMsgTimeout() {
        try {
            int count = 0;
            long minTime = Client.clock() - msgTimeout - 10000;
            //could make inFlight a LinkedHashMap and only iterate through some of it
            for (Iterator<Long> iter = inFlight.values().iterator(); iter.hasNext(); ) {
                if (iter.next() < minTime) {
                    iter.remove(); //nsqd will requeue it, don't send REQ, just drop it
                    count++;
                }
            }
            if (count > 0) {
                logger.info("{} msgs timed out", count);
                checkReady();
            }
        }
        catch (Exception e) {
            logger.error("checkMsgTimeout error", e);
        }
    }

    private void checkFlush() throws IOException {
        if (unflushedCount >= maxUnflushed) {
            logger.debug("max unflushed hit");
            flush();
        }
        else {
            unflushedCount++;
        }
    }

    public synchronized void setMaxInFlight(int maxInFlight) {
        try {
            this.maxInFlight = maxInFlight;
            maxUnflushed = maxInFlight / 3;
            if (rdy > maxInFlight - inFlight.size()) {
                sendReady(); //maxInFlight decreased, possibly too many in flight
            }
            else {
                checkReady();
            }
        }
        catch (IOException e) {
            logger.error("setMaxInFlight failed", e);
            close();
        }
    }

    public synchronized int getMaxInFlight() {
        return maxInFlight;
    }

    private void checkReady() throws IOException {
        logger.debug("checkReady rdy:{} maxInFlight:{} inFlight:{}", rdy, maxInFlight, inFlight.size());
        if (rdy < maxInFlight / 3.0 && inFlight.size() < maxInFlight / 2.0) {
            sendReady();
        }
    }

    private void sendReady() throws IOException {
        rdy = Math.max(0, maxInFlight - inFlight.size());
        logger.debug("SEND RDY:{}", rdy);
        writeCommand("RDY", rdy);
        flush();
    }

    @Override
    public synchronized void connect(Config config) throws IOException {
        super.connect(config);
        writeCommand("SUB", subscription.getTopic(), subscription.getChannel());
        flushAndReadOK();
        checkReady();
    }

    @Override
    protected void onMessage(long timestamp, int attempts, String id, byte[] data) {
        final NSQMessage msg = new NSQMessage(timestamp, attempts, id, data, this);
        synchronized (this) {
            inFlight.put(id, Client.clock());
            rdy--;
        }
        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    handler.accept(msg);
                }
                catch (Throwable t) {
                    logger.error("msg error", t);
                }
            }
        });
    }

}
