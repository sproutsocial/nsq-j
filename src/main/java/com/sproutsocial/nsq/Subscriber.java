package com.sproutsocial.nsq;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

public class Subscriber extends Client {

    private final List<HostAndPort> lookups = Lists.newArrayList();
    private final List<Subscription> subscriptions = Lists.newArrayList();
    private ScheduledFuture lookupTask;
    private int lookupIntervalSecs;
    private int maxInFlightPerSubscription = 200;
    private int maxFlushDelayMillis = 4000;
    private ExecutorService executor = defaultExecutor;

    private static final ExecutorService defaultExecutor = new ThreadPoolExecutor(1, 6, 300, TimeUnit.SECONDS,
                                            new LinkedBlockingQueue<Runnable>(), Util.threadFactory("nsq-sub"));

    private static final Logger logger = LoggerFactory.getLogger(Subscriber.class);

    public Subscriber(String... lookupHosts) {
        for (String h : lookupHosts) {
            lookups.add(HostAndPort.fromString(h).withDefaultPort(4161));
        }
        setLookupIntervalSecs(60);
    }

    public synchronized void subscribe(String topic, String channel, MessageHandler handler) {
        Subscription sub = new Subscription(topic, channel, handler, this);
        subscriptions.add(sub);
        sub.checkConnections(lookupTopic(topic));
    }

    private synchronized void lookup() {
        for (Subscription sub : subscriptions) {
            sub.checkConnections(lookupTopic(sub.getTopic()));
        }
    }

    protected Set<HostAndPort> lookupTopic(String topic) {
        Set<HostAndPort> nsqds = new HashSet<HostAndPort>();
        for (HostAndPort lookup : lookups) {
            try {
                URL url = new URL(String.format("http://%s/lookup?topic=%s", lookup, topic));
                HttpURLConnection con = (HttpURLConnection) url.openConnection();
                if (con.getResponseCode() != 200) {
                    logger.debug("ignoring lookup resp:{} nsqlookupd:{} topic:{}", con.getResponseCode(), lookup, topic);
                    continue;
                }
                JsonNode root = Client.mapper.readTree(con.getInputStream()); //don't need another buffer here, jackson buffers
                JsonNode producers = root.get("data").get("producers");
                for (int i = 0; i < producers.size(); i++) {
                    JsonNode prod = producers.get(i);
                    nsqds.add(HostAndPort.fromParts(prod.get("broadcast_address").asText(), prod.get("tcp_port").asInt()));
                }
                con.getInputStream().close();
            }
            catch (Exception e) {
                logger.error("lookup error, ignoring nsqlookupd:{} topic:{}", lookup, topic, e);
            }
        }
        logger.debug("lookup topic:{} result:{}", topic, nsqds);
        return nsqds;
    }

    public synchronized void stop(int waitExecutorSecs) {
        Util.cancel(lookupTask);
        lookupTask = null;
        for (Subscription subscription : subscriptions) {
            //TODO don't close (need to FIN), send RDY 0 and wait for inFlight -> 0
            subscription.close();
        }
        //if (waitExecutorSecs > 0) {
        //    executor.shutdown();
        //    executor.awaitTermination(waitExecutorSecs, TimeUnit.SECONDS);
        //}
        logger.info("subscriber stopped");
    }

    public synchronized int getLookupIntervalSecs() {
        return lookupIntervalSecs;
    }

    public synchronized void setLookupIntervalSecs(int lookupIntervalSecs) {
        this.lookupIntervalSecs = lookupIntervalSecs;
        Util.cancel(lookupTask);
        lookupTask = Client.scheduleAtFixedRate(new Runnable() {
            public void run() {
                lookup();
            }
        }, lookupIntervalSecs * 1000, lookupIntervalSecs * 1000);
    }

    public synchronized int getMaxInFlightPerSubscription() {
        return maxInFlightPerSubscription;
    }

    public synchronized void setMaxInFlightPerSubscription(int maxInFlightPerSubscription) {
        this.maxInFlightPerSubscription = maxInFlightPerSubscription;
    }

    public synchronized ExecutorService getExecutor() {
        return executor;
    }

    public synchronized void setExecutor(ExecutorService executor) {
        this.executor = executor;
    }

    public synchronized int getMaxFlushDelayMillis() {
        return maxFlushDelayMillis;
    }

    public synchronized void setMaxFlushDelayMillis(int maxFlushDelayMillis) {
        this.maxFlushDelayMillis = maxFlushDelayMillis;
    }

}
