package com.sproutsocial.nsq;

import com.google.common.net.HostAndPort;
import net.jcip.annotations.GuardedBy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

class SubConnection extends Connection {

    private final MessageHandler handler;
    private final FailedMessageHandler failedMessageHandler;
    private final Subscription subscription;
    private final String topic;
    private final int maxAttemps;
    private final int maxFlushDelayMillis;
    private int inFlight = 0;
    private int maxInFlight = 0;
    private int maxUnflushed = 0;

    private long finishedCount = 0;
    private long requeuedCount = 0;

    private static final Logger logger = LoggerFactory.getLogger(SubConnection.class);

    public SubConnection(Client client, HostAndPort host, Subscription subscription) {
        super(client, host);
        Subscriber subscriber = subscription.getSubscriber();
        this.handler = subscription.getHandler();
        this.failedMessageHandler = subscriber.getFailedMessageHandler();
        this.subscription = subscription;
        this.topic = subscription.getTopic();
        this.maxAttemps = subscriber.getMaxAttempts();
        this.maxFlushDelayMillis = subscriber.getMaxFlushDelayMillis();

        scheduleAtFixedRate(new Runnable() {
            public void run() {
                delayedFlush();
            }
        }, maxFlushDelayMillis / 2, maxFlushDelayMillis / 2, false);
    }

    public synchronized void finish(String id) {
        try {
            writeCommand("FIN", id);
            finishedCount++;
            messageDone();
        }
        catch (IOException e) {
            logger.error("finish error. {}", stateDesc(), e);
            close();
        }
    }

    public synchronized void requeue(String id) {
        try {
            writeCommand("REQ", id, 0);
            requeuedCount++;
            messageDone();
        }
        catch (IOException e) {
            logger.error("requeue error. {}", stateDesc(), e);
            close();
        }
    }

    @GuardedBy("this")
    private void messageDone() throws IOException {
        inFlight--;
        if (inFlight == 0 && isStopping) {
            flushAndClose();
        }
        else {
            checkFlush();
        }
    }

    public synchronized void touch(String id) {
        try {
            writeCommand("TOUCH", id);
            checkFlush();
        }
        catch (IOException e) {
            logger.error("touch error. {}", stateDesc(), e);
            close();
        }
    }

    private synchronized void delayedFlush() {
        try {
            if (unflushedCount > 0 && Util.clock() - lastActionFlush > (maxFlushDelayMillis / 2) + 10) {
                flush();
            }
        }
        catch (Exception e) {
            logger.error("delayedFlush error. {}", stateDesc(), e);
            close();
        }
    }

    @GuardedBy("this")
    private void checkFlush() throws IOException {
        if (unflushedCount >= maxUnflushed) {
            flush();
        }
        else {
            unflushedCount++;
        }
    }

    public synchronized void setMaxInFlight(int maxInFlight) {
        setMaxInFlight(maxInFlight, true);
    }

    public synchronized void setMaxInFlight(int maxInFlight, boolean isActive) {
        try {
            if (this.maxInFlight == maxInFlight) {
                return;
            }
            this.maxInFlight = maxInFlight;
            maxUnflushed = Math.min(maxInFlight / 3, 150); //should this be configurable?  FIN id\n is 21 bytes
            logger.debug("RDY:{} {}", maxInFlight, toString());
            writeCommand("RDY", maxInFlight);
            if (isActive) {
                flush();
            }
            else {
                out.flush(); //don't update lastActionFlush time - keep connection inactive
            }
        }
        catch (IOException e) {
            logger.error("setMaxInFlight failed. con:{}", stateDesc(), e);
            close();
        }
    }

    public synchronized int getMaxInFlight() {
        return maxInFlight;
    }

    @Override
    public synchronized void connect(Config config) throws IOException {
        client.addSubConnection(this);
        super.connect(config);
        writeCommand("SUB", subscription.getTopic(), subscription.getChannel());
        flushAndReadOK();
    }

    private void failMessage(final NSQMessage msg) {
        if (failedMessageHandler != null) {
            handlerExecutor.execute(new Runnable() {
                public void run() {
                    try {
                        failedMessageHandler.failed(subscription.getTopic(), subscription.getChannel(), msg);
                    }
                    catch (Throwable t) {
                        logger.error("failed message error", t);
                    }
                }
            });
        }
        finish(msg.getId());
    }

    @Override
    protected void onMessage(long timestamp, int attempts, String id, byte[] data) {
        final NSQMessage msg = new NSQMessage(timestamp, attempts, id, data, topic, this);
        synchronized (this) {
            inFlight++;
        }
        if (msg.getAttempts() >= maxAttemps) {
            failMessage(msg);
        }
        else {
            handlerExecutor.execute(new Runnable() {
                public void run() {
                    try {
                        handler.accept(msg);
                    }
                    catch (Throwable t) {
                        logger.error("message error", t);
                    }
                }
            });
        }
    }

    @Override
    public void close() {
        super.close();
        //be paranoid about locks, we only care that this happens sometime soon
        client.getSchedExecutor().execute(new Runnable() {
            public void run() {
                subscription.connectionClosed(SubConnection.this);
                client.connectionClosed(SubConnection.this);
            }
        });
    }

    @Override
    public synchronized void stop() {
        super.stop();
        if (inFlight == 0) {
            flushAndClose();
        }
        else {
            setMaxInFlight(0);
        }
    }

    @Override
    public String toString() {
        return String.format("SubCon:%s %s.%s", host.getHost(), subscription.getTopic(), subscription.getChannel());
    }

    @Override
    public synchronized String stateDesc() {
        return String.format("%s inFlight:%d maxInFlight:%d fin:%d req:%d",
                super.stateDesc(), inFlight, maxInFlight, finishedCount, requeuedCount);
    }

}
