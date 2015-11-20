package com.sproutsocial.nsq;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.google.common.collect.Sets;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.MoreExecutors;
import com.sproutsocial.nsq.jmx.ClientMXBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.*;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class Client {

    static final ObjectMapper mapper = new ObjectMapper();
    static final EventBus eventBus = new EventBus();

    private static final Set<Publisher> publishers = Collections.newSetFromMap(new ConcurrentHashMap<Publisher, Boolean>());
    private static final Set<Subscriber> subscribers = Collections.newSetFromMap(new ConcurrentHashMap<Subscriber, Boolean>());
    private static final Set<SubConnection> subConnections = Collections.newSetFromMap(new ConcurrentHashMap<SubConnection, Boolean>());
    private static int subConnectionCount = 0;

    private static final NSQClient instance = new NSQClient();
    private static ExecutorService executor;
    private static final ScheduledExecutorService schedExecutor = Executors.newSingleThreadScheduledExecutor(Util.threadFactory("nsq-sched"));
    private static final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();

    private static final Logger logger = LoggerFactory.getLogger(Client.class);

    static {
        mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        mapper.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);
        eventBus.register(instance);
        registerMBean(instance, "nsq", "client");
    }

    //--------------------------

    /**
     * Stops all subscribers, waits for in-flight messages to be finished or requeued, stops the executor that handles messages,
     * then stops all publishers. All connections will be closed and no threads started by this client should be running when this returns.
     * @param waitMillis Time to wait for everything to stop, in milliseconds. Soft limit that may be exceeded by about 200 ms.
     */
    public static synchronized void stop(int waitMillis) {
        instance.stopAll(waitMillis);
    }

    /**
     * Stops all subscribers, waits for in-flight messages to be finished or requeued, then closes subscriber connections.
     * Useful if you need to perform some action before publishers are stopped,
     * you should call stop() after this to shutdown all threads.
     * @param waitMillis Time to wait for in-flight messages to be finished, in milliseconds.
     */
    public static synchronized void stopSubscribers(int waitMillis) {
        instance.stopSub(waitMillis);
    }

    public static synchronized void setExecutor(ExecutorService executor) {
        checkNotNull(executor);
        checkState(Client.executor == null, "executor can only be set once");
        Client.executor = executor;
    }

    public static synchronized ExecutorService getExecutor() {
        if (executor == null) {
            executor = Executors.newFixedThreadPool(6, Util.threadFactory("nsq-sub"));
        }
        return executor;
    }

    public static int getCurrentConnectionCount() {
        return instance.getCurrentConnectionCount();
    }

    public static int getTotalDisconnections() {
        return instance.getTotalDisconnections();
    }

    public static long getPublishedCount() {
        return instance.getPublishedCount();
    }

    public static long getPublishedFailoverCount() {
        return instance.getPublishedFailoverCount();
    }

    public static long getPublishFailedCount() {
        return instance.getPublishFailedCount();
    }

    //--------------------------

    static void addPublisher(Publisher publisher) {
        if (!publishers.contains(publisher)) {
            registerMBean(publisher, "nsq.publisher", "pub" + publishers.size());
            publishers.add(publisher);
        }
    }

    static void addSubscriber(Subscriber subscriber) {
        if (!subscribers.contains(subscriber)) {
            registerMBean(subscriber, "nsq.subscriber", "sub" + subscribers.size());
            subscribers.add(subscriber);
        }
    }

    static void addSubConnection(SubConnection subCon) {
        if (!subConnections.contains(subCon)) {
            subCon.setSubConId(subConnectionCount);
            registerMBean(subCon, "nsq." + subCon.getTopicChannelString(), subCon.getName());
            subConnections.add(subCon);
            subConnectionCount++;
        }
    }

    private static void registerMBean(Object obj, String domain, String name) {
        try {
            mbeanServer.registerMBean(obj, new ObjectName(domain + ":type=" + name));
        }
        catch (Exception e) {
            logger.error("failed to register mbean:{}", domain + ":type=" + name, e);
        }
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

    //--------------------------

    //inner class to hide connectionClosed which must be public
    static class NSQClient implements ClientMXBean {

        private int disconnectionCount;

        private synchronized void stopSub(int waitMillis) {
            for (Subscriber subscriber : subscribers) {
                subscriber.stop();
            }
            long start = clock();
            while (clock() - start < waitMillis && !subConnections.isEmpty()) {
                logger.info("waiting for subscribers to finish in-flight messages");
                try {
                    wait(waitMillis);
                }
                catch (InterruptedException e) {
                }
            }
            for (SubConnection subCon : subConnections) {
                subCon.close();
            }
        }

        private synchronized void stopAll(int waitMillis) {
            logger.info("stopping nsq client");
            long start = clock();
            stopSub(waitMillis);

            if (executor != null && !executor.isTerminated()) {
                int timeout = Math.max((int) (waitMillis - (clock() - start)), 100);
                MoreExecutors.shutdownAndAwaitTermination(executor, timeout, TimeUnit.MILLISECONDS);
            }

            for (Publisher publisher : publishers) {
                publisher.stop();
            }

            int timeout = Math.max((int) (waitMillis - (clock() - start)), 100);
            MoreExecutors.shutdownAndAwaitTermination(schedExecutor, timeout, TimeUnit.MILLISECONDS);

            logger.debug("executor.isTerminated:{} schedExecutor.isTerminated:{}", executor != null ? executor.isTerminated() : "null", schedExecutor.isTerminated());
            logger.info("nsq client stopped");
        }

        @Subscribe
        public synchronized void connectionClosed(Connection closedCon) {
            disconnectionCount++;
            if (closedCon instanceof SubConnection) {
                if (subConnections.remove(closedCon)) {
                    SubConnection subCon = (SubConnection) closedCon;
                    String jmxName = "nsq." + subCon.getTopicChannelString() + ":type=" + subCon.getName();
                    try {
                        mbeanServer.unregisterMBean(new ObjectName(jmxName));
                    }
                    catch (Exception e) {
                        logger.error("failed to unregister mbean:{}", jmxName, e);
                    }
                }
                if (subConnections.isEmpty()) {
                    notifyAll();
                }
            }
        }

        @Override
        public synchronized int getCurrentConnectionCount() {
            int count = 0;
            for (Publisher publisher : publishers) {
                if (publisher.isConnected()) {
                    count++;
                }
            }
            for (Subscriber subscriber : subscribers) {
                count += subscriber.getConnectionCount();
            }
            return count;
        }

        @Override
        public synchronized int getTotalDisconnections() {
            return disconnectionCount;
        }

        @Override
        public synchronized long getPublishedCount() {
            int count = 0;
            for (Publisher publisher : publishers) {
                count += publisher.getPublishedCount();
            }
            return count;
        }

        @Override
        public synchronized long getPublishedFailoverCount() {
            int count = 0;
            for (Publisher publisher : publishers) {
                count += publisher.getPublishedFailoverCount();
            }
            return count;
        }

        @Override
        public synchronized long getPublishFailedCount() {
            int count = 0;
            for (Publisher publisher : publishers) {
                count += publisher.getPublishFailedCount();
            }
            return count;
        }

        @Override
        public int getFailedMessageCount() {
            int count = 0;
            for (Subscriber subscriber : subscribers) {
                count += subscriber.getFailedMessageCount();
            }
            return count;
        }

        @Override
        public int getHandlerErrorCount() {
            int count = 0;
            for (Subscriber subscriber : subscribers) {
                count += subscriber.getHandlerErrorCount();
            }
            return count;
        }
    }

}
