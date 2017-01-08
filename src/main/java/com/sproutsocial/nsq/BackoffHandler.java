package com.sproutsocial.nsq;

import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class BackoffHandler implements MessageHandler {

    private volatile boolean isBackoff = false;
    private Subscription subscription;
    private final MessageHandler handler;
    private final int initDelay;
    private final int maxDelay;

    private long lastAttempt;
    private int delay;
    private int failCount;
    private int fullSpeedMaxInFlight;

    private static final int DEFAULT_INIT_DELAY_MILLIS = 1000;
    private static final int DEFAULT_MAX_DELAY_MILLIS = 60000;

    private static final Logger logger = LoggerFactory.getLogger(BackoffHandler.class);

    public BackoffHandler(MessageHandler handler, int initDelayMillis, int maxDelayMillis) {
        this.handler = handler;
        this.initDelay = initDelayMillis;
        this.maxDelay = maxDelayMillis;
    }

    public BackoffHandler(MessageHandler handler) {
        this(handler, DEFAULT_INIT_DELAY_MILLIS, DEFAULT_MAX_DELAY_MILLIS);
    }

    @Override
    public void accept(Message msg) {
        boolean backoff = isBackoff;
        if (backoff) {
            attemptDuringBackoff();
        }
        try {
            handler.accept(msg);
            if (backoff) {
                successDuringBackoff();
            }
            msg.finish();
        }
        catch (Exception e) { //throwable?
            failure(msg, e);
        }
    }

    synchronized void setSubscription(Subscription subscription) {
        this.subscription = subscription;
    }

    private synchronized void failure(Message msg, Exception e) {
        isBackoff = true;
        failCount++;
        logger.error("message error. failures:{}", failCount, e);
        if (failCount == 1) {
            delay = initDelay;
            fullSpeedMaxInFlight = subscription.getMaxInFlight();
            lastAttempt = Util.clock();
        }
        else {
            delay = Math.min(delay * 2, maxDelay);
            pauseSubscription();
        }
        msg.requeue();
    }

    private synchronized void pauseSubscription() {
        subscription.setMaxInFlight(0);
        subscription.getClient().schedule(new Runnable() {
            public void run() {
                if (!subscription.isStopping) {
                    subscription.setMaxInFlight(1);
                }
            }
        }, delay);
    }

    private synchronized void attemptDuringBackoff() {
        long now = Util.clock();
        int waited = (int) (now - lastAttempt);
        if (waited < delay) {
            Util.sleepQuietly(delay - waited);
            lastAttempt = Util.clock();
        }
        else {
            lastAttempt = now;
        }
    }

    private synchronized void successDuringBackoff() {
        delay /= 2;
        if (delay < initDelay) {
            isBackoff = false;
            failCount = 0;
            delay = 0;
            subscription.setMaxInFlight(fullSpeedMaxInFlight);
        }
        else {
            pauseSubscription();
        }
    }

}
