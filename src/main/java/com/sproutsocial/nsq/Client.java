package com.sproutsocial.nsq;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.*;

import static com.google.common.base.Preconditions.*;

/**
 * Thread safe
 */
public class Client {

    static final ObjectMapper mapper = new ObjectMapper();

    private static final Set<Publisher> publishers = Collections.newSetFromMap(new ConcurrentHashMap<Publisher, Boolean>());
    private static final Set<Subscriber> subscribers = Collections.newSetFromMap(new ConcurrentHashMap<Subscriber, Boolean>());
    private static final Set<SubConnection> subConnections = Collections.newSetFromMap(new ConcurrentHashMap<SubConnection, Boolean>());
    private static final Object subConMonitor = new Object();

    private static ExecutorService executor;
    private static final ScheduledExecutorService schedExecutor = Executors.newSingleThreadScheduledExecutor(Util.threadFactory("nsq-sched"));

    private static final Logger logger = LoggerFactory.getLogger(Client.class);

    static {
        mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        mapper.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);
    }

    //--------------------------

    /**
     * Stops all subscribers, waits for in-flight messages to be finished or requeued, stops the executor that handles messages,
     * then stops all publishers. All connections will be closed and no threads started by this client should be running when this returns.
     * @param waitMillis Time to wait for everything to stop, in milliseconds. Soft limit that may be exceeded by about 200 ms.
     */
    public static synchronized boolean stop(int waitMillis) {
        checkArgument(waitMillis > 0, "waitMillis must be greater than zero");
        logger.info("stopping nsq client");
        boolean isClean = true;
        long start = clock();
        isClean &= stopSubscribers(waitMillis);

        if (executor != null && !executor.isTerminated()) {
            int timeout = Math.max((int) (waitMillis - (clock() - start)), 100);
            isClean &= MoreExecutors.shutdownAndAwaitTermination(executor, timeout, TimeUnit.MILLISECONDS);
        }

        for (Publisher publisher : publishers) {
            publisher.stop();
        }

        int timeout = Math.max((int) (waitMillis - (clock() - start)), 100);
        isClean &= MoreExecutors.shutdownAndAwaitTermination(schedExecutor, timeout, TimeUnit.MILLISECONDS);

        logger.debug("executor.isTerminated:{} schedExecutor.isTerminated:{} isClean:{}", executor != null ? executor.isTerminated() : "null", schedExecutor.isTerminated(), isClean);
        logger.info("nsq client stopped");
        return isClean;
    }

    /**
     * Stops all subscribers, waits for in-flight messages to be finished or requeued, then closes subscriber connections.
     * Useful if you need to perform some action before publishers are stopped,
     * you should call stop() after this to shutdown all threads.
     * @param waitMillis Time to wait for in-flight messages to be finished, in milliseconds.
     */
    public static synchronized boolean stopSubscribers(int waitMillis) {
        checkArgument(waitMillis > 0, "waitMillis must be greater than zero");
        for (Subscriber subscriber : subscribers) {
            subscriber.stop();
        }
        synchronized (subConMonitor) {
            if (!subConnections.isEmpty()) { //don't loop until empty, try once and if we get interrupted stop right away
                logger.info("waiting for subscribers to finish in-flight messages");
                try {
                    subConMonitor.wait(waitMillis);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        boolean isClean = subConnections.isEmpty();
        for (SubConnection subCon : subConnections) {
            subCon.close();
        }
        return isClean;
    }

    public static synchronized void setExecutor(ExecutorService executor) {
        checkNotNull(executor);
        checkState(Client.executor == null, "executor can only be set once, must be set before subscribing");
        Client.executor = executor;
    }

    public static synchronized ExecutorService getExecutor() {
        if (executor == null) {
            executor = Executors.newFixedThreadPool(6, Util.threadFactory("nsq-sub"));
        }
        return executor;
    }

    //--------------------------
    // package private

    static void addPublisher(Publisher publisher) {
        publishers.add(publisher);
    }

    static void addSubscriber(Subscriber subscriber) {
        subscribers.add(subscriber);
    }

    static void addSubConnection(SubConnection subCon) {
        subConnections.add(subCon);
    }

    static long clock() {
        return System.nanoTime() / 1000000;
    }

    static ScheduledFuture scheduleAtFixedRate(final Runnable runnable, int initialDelay, int period, boolean jitter) {
        if (jitter) {
            initialDelay = (int) (initialDelay * 0.1 + Math.random() * initialDelay * 0.9);
        }
        return schedExecutor.scheduleAtFixedRate(new Runnable() {
            public void run() {
                try {
                    runnable.run();
                }
                catch (Throwable t) {
                    logger.error("task error", t);
                }
            }
        }, initialDelay, period, TimeUnit.MILLISECONDS);
    }

    static void connectionClosed(SubConnection closedCon) {
        synchronized (subConMonitor) {
            subConnections.remove(closedCon);
            if (subConnections.isEmpty()) {
                subConMonitor.notifyAll();
            }
        }
    }

}
