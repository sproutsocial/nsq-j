package com.sproutsocial.nsq;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

@ThreadSafe
public class Subscriber extends BasePubSub {

    private final List<HostAndPort> lookups = Lists.newArrayList();
    private final List<Subscription> subscriptions = Lists.newArrayList();
    private final int lookupIntervalSecs;
    private int maxLookupFailuresBeforeError;
    private int defaultMaxInFlight = 200;
    private int maxFlushDelayMillis = 2000;
    private int maxAttempts = Integer.MAX_VALUE;
    private FailedMessageHandler failedMessageHandler = null;
    private final Map<String, Integer> failures = new HashMap<String, Integer>();

    private static final int DEFAULT_LOOKUP_INTERVAL_SECS = 60;
    private static final int DEFAULT_MAX_LOOKUP_FAILURES_BEFORE_ERROR = 5;

    private static final Logger logger = LoggerFactory.getLogger(Subscriber.class);

    public Subscriber(Client client, int lookupIntervalSecs, int maxLookupFailuresBeforeError, String... lookupHosts) {
        super(client);
        checkArgument(lookupIntervalSecs > 0, "lookupIntervalSecs must be greater than zero");
        this.lookupIntervalSecs = lookupIntervalSecs;
        this.maxLookupFailuresBeforeError = maxLookupFailuresBeforeError;
        client.scheduleAtFixedRate(new Runnable() {
                public void run() {
                    lookup();
                }
            }, lookupIntervalSecs * 1000, lookupIntervalSecs * 1000, true);
        for (String h : lookupHosts) {
            lookups.add(HostAndPort.fromString(h).withDefaultPort(4161));
        }
    }

    public Subscriber(int lookupIntervalSecs, String... lookupHosts) {
        this(Client.getDefaultClient(), lookupIntervalSecs, DEFAULT_MAX_LOOKUP_FAILURES_BEFORE_ERROR, lookupHosts);
    }

    public Subscriber(String... lookupHosts) {
        this(Client.getDefaultClient(), DEFAULT_LOOKUP_INTERVAL_SECS, DEFAULT_MAX_LOOKUP_FAILURES_BEFORE_ERROR,
                lookupHosts);
    }

    public synchronized void subscribe(String topic, String channel, MessageHandler handler) {
        subscribe(topic, channel, defaultMaxInFlight, handler);
    }

    /**
     * Subscribe to a topic.
     * If the configured executor is multi-threaded and maxInFlight > 1 (the defaults)
     * then the MessageHandler must be thread safe.
     */
    public synchronized void subscribe(String topic, String channel, int maxInFlight, MessageHandler handler) {
        checkNotNull(topic);
        checkNotNull(channel);
        checkNotNull(handler);
        client.addSubscriber(this);
        Subscription sub = new Subscription(client, topic, channel, handler, this, maxInFlight);
        if (handler instanceof BackoffHandler) {
            ((BackoffHandler)handler).setSubscription(sub); //awkward
        }
        subscriptions.add(sub);
        sub.checkConnections(lookupTopic(topic));
    }

    public synchronized void setMaxInFlight(String topic, String channel, int maxInFlight) {
        for (Subscription sub : subscriptions) {
            if (sub.getTopic().equals(topic) && sub.getChannel().equals(channel)) {
                sub.setMaxInFlight(maxInFlight);
            }
        }
    }

    private synchronized void lookup() {
        if (isStopping) {
            return;
        }
        for (Subscription sub : subscriptions) {
            sub.checkConnections(lookupTopic(sub.getTopic()));
        }
    }

    @GuardedBy("this")
    protected Set<HostAndPort> lookupTopic(String topic) {
        Set<HostAndPort> nsqds = new HashSet<HostAndPort>();
        for (HostAndPort lookup : lookups) {
            String urlString = String.format("http://%s/lookup?topic=%s", lookup, topic);
            try {
                URL url = new URL(urlString);
                HttpURLConnection con = (HttpURLConnection) url.openConnection();
                if (con.getResponseCode() != 200) {
                    logger.debug("ignoring lookup resp:{} nsqlookupd:{} topic:{}", con.getResponseCode(), lookup, topic);
                    continue;
                }
                JsonNode root = client.getObjectMapper().readTree(con.getInputStream()); //don't need another buffer here, jackson buffers
                if (root.has("data")) {
                    root = root.get("data"); //nsq before version 1.0 wrapped the response with status_code/data
                }
                JsonNode producers = root.get("producers");
                for (int i = 0; i < producers.size(); i++) {
                    JsonNode prod = producers.get(i);
                    nsqds.add(HostAndPort.fromParts(prod.get("broadcast_address").asText(), prod.get("tcp_port").asInt()));
                }
                con.getInputStream().close();
                this.failures.remove(urlString);
            } catch (Exception e) {
                Integer lookupFailureCount = this.failures.get(urlString);
                if (lookupFailureCount == null) {
                    lookupFailureCount = 0;
                }
                lookupFailureCount++;
                this.failures.put(urlString, lookupFailureCount);

                if (lookupFailureCount >= this.maxLookupFailuresBeforeError) {
                    logger.error("lookup failure. lookup failed for {} consecutive tries. nsqlookupd:{} topic:{}",
                            lookupFailureCount, lookup, topic, e);
                } else {
                    logger.warn("lookup failure. lookup failed for {} consecutive tries. nsqlookupd:{} topic:{}",
                            lookupFailureCount, lookup, topic, e);
                }
            }
        }
        //logger.debug("lookup topic:{} result:{}", topic, nsqds);
        return nsqds;
    }

    @Override
    public void stop() {
        super.stop();
        for (Subscription subscription : subscriptions) {
            subscription.stop();
        }
        logger.info("subscriber stopped");
    }

    public synchronized int getDefaultMaxInFlight() {
        return defaultMaxInFlight;
    }

    /**
     * the maxInFlight to use for new subscriptions
     */
    public synchronized void setDefaultMaxInFlight(int defaultMaxInFlight) {
        this.defaultMaxInFlight = defaultMaxInFlight;
    }

    public synchronized int getMaxFlushDelayMillis() {
        return maxFlushDelayMillis;
    }

    public synchronized void setMaxFlushDelayMillis(int maxFlushDelayMillis) {
        this.maxFlushDelayMillis = maxFlushDelayMillis;
    }

    public synchronized int getMaxAttempts() {
        return maxAttempts;
    }

    public synchronized void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public synchronized FailedMessageHandler getFailedMessageHandler() {
        return failedMessageHandler;
    }

    public synchronized void setFailedMessageHandler(FailedMessageHandler failedMessageHandler) {
        this.failedMessageHandler = failedMessageHandler;
    }

    public synchronized int getLookupIntervalSecs() {
        return lookupIntervalSecs;
    }

    public Integer getExecutorQueueSize() {
        ExecutorService executor = client.getExecutor();
        return executor instanceof ThreadPoolExecutor ? ((ThreadPoolExecutor)executor).getQueue().size() : null;
    }

    public synchronized int getConnectionCount() {
        int count = 0;
        for (Subscription subscription : subscriptions) {
            count += subscription.getConnectionCount();
        }
        return count;
    }

}
