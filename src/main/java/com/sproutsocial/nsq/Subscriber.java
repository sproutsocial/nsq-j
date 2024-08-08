package com.sproutsocial.nsq;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import static com.sproutsocial.nsq.Util.checkArgument;
import static com.sproutsocial.nsq.Util.checkNotNull;

@ThreadSafe
public class Subscriber extends BasePubSub {

    private final List<HostAndPort> lookups = new ArrayList<HostAndPort>();
    private final List<Subscription> subscriptions = new ArrayList<Subscription>();
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
        checkArgument(lookupIntervalSecs > 0);
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

    public synchronized void subscribe(String topic, String channel, final MessageDataHandler handler) {
        subscribe(topic, channel, defaultMaxInFlight, new BackoffHandler(new MessageHandler() {
            @Override
            public void accept(Message msg) {
                handler.accept(msg.getData());
            }
        }));
    }

    /*
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

    /**
     * Unsubscribe from the current topic / channel subscription. This will stop the flow of messages to the
     * previously registered message handler.
     *
     * NOTE: This will *not* delete the underlying channel that might have been created during the initial subscribe
     * call.
     */
    public synchronized boolean unsubscribe(String topic, String channel) {
        return unsubscribeSubscription(topic, channel) != null;
    }

    synchronized Subscription unsubscribeSubscription(String topic, String channel) {
        for (int i = 0; i < subscriptions.size(); i++) {
            final Subscription sub = subscriptions.get(i);
            if (sub.getTopic().equals(topic) && sub.getChannel().equals(channel)) {
                sub.stop();
                return subscriptions.remove(i);
            }
        }
        return null;
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
            String urlString = null;
            BufferedReader in = null;
            try {
                urlString = String.format("http://%s/lookup?topic=%s", lookup, URLEncoder.encode(topic, "UTF-8"));
                URL url = new URL(urlString);
                HttpURLConnection con = (HttpURLConnection) url.openConnection();
                con.setConnectTimeout(30000);
                con.setReadTimeout(30000);
                if (con.getResponseCode() != 200) {
                    logger.debug("ignoring lookup resp:{} nsqlookupd:{} topic:{}", con.getResponseCode(), lookup, topic);
                    continue;
                }
                in = new BufferedReader(new InputStreamReader(con.getInputStream()));
                LookupResponse resp = client.getGson().fromJson(in, LookupResponse.class);
                if (resp.getData() != null) {
                    resp = resp.getData(); //nsq before version 1.0 wrapped the response with status_code/data
                }
                for (LookupResponse.Producer prod : resp.getProducers()) {
                    nsqds.add(HostAndPort.fromParts(prod.getBroadcastAddress(), prod.getTcpPort()));
                }
                this.failures.remove(urlString);
            }
            catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
            catch (Exception e) {
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
            finally {
                Util.closeQuietly(in);
            }
        }
        return nsqds;
    }

    @Override
    public void stop() {
        super.stop();
        for (Subscription subscription : subscriptions) {
            subscription.stop();
        }
        subscriptions.clear();
        logger.info("subscriber stopped");
    }

    public synchronized int getDefaultMaxInFlight() {
        return defaultMaxInFlight;
    }

    /*
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

    /**
     * Return the currently in-flight message count for all active
     * subscriptions. This count represents the number of messages that
     * are currently being processed by the the executor service handler
     * threads.
     */
    public synchronized int getCurrentInFlightCount() {
        int inFlightCount = 0;
        for (Subscription subscription : subscriptions) {
            inFlightCount += subscription.getInFlightCount();
        }
        return inFlightCount;
    }

    public synchronized int getConnectionCount() {
        int count = 0;
        for (Subscription subscription : subscriptions) {
            count += subscription.getConnectionCount();
        }
        return count;
    }

}
