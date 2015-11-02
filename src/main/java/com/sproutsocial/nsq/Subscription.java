package com.sproutsocial.nsq;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.eventbus.Subscribe;
import com.google.common.net.HostAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ScheduledFuture;

class Subscription {

    private final String topic;
    private final String channel;
    private final MessageHandler handler;
    private final Subscriber subscriber;
    private final Map<HostAndPort, SubConnection> connectionMap = Maps.newHashMap();
    private ScheduledFuture lowFlightRotateTask;

    private static final Logger logger = LoggerFactory.getLogger(Subscription.class);

    public Subscription(String topic, String channel, MessageHandler handler, Subscriber subscriber) {
        this.topic = topic;
        this.channel = channel;
        this.handler = handler;
        this.subscriber = subscriber;
        Client.eventBus.register(this);
    }

    public synchronized void checkConnections(Set<HostAndPort> activeHosts) {
        for (Iterator<SubConnection> iter = connectionMap.values().iterator(); iter.hasNext();) {
            SubConnection con  = iter.next();
            if (!activeHosts.contains(con.getHost())) {
                if (Client.clock() - con.getLastActionFlush() > con.getMsgTimeout() * 100) {
                    logger.info("closing inactive connection:{} topic:{}", con.getHost(), topic);
                    iter.remove();
                    con.close();
                }
            }
        }
        for (HostAndPort activeHost : activeHosts) {
            if (!connectionMap.containsKey(activeHost)) {
                try {
                    logger.info("adding new connection:{} topic:{}", activeHost, topic);
                    SubConnection con = new SubConnection(activeHost, this);
                    con.connect(subscriber.getConfig());
                    connectionMap.put(activeHost, con);
                }
                catch (IOException e) {
                    logger.error("error connecting to:{}", activeHost, e);
                }
            }
        }
        distributeMaxInFlight();
    }

    private void distributeMaxInFlight() {
        if (checkLowFlight() || connectionMap.isEmpty()) {
            return;
        }
        List<SubConnection> activeCons = Lists.newArrayList();
        List<SubConnection> inactiveCons = Lists.newArrayList();
        long minActiveTime = Client.clock() - subscriber.getLookupIntervalSecs() * 1000 - 5000;
        for (SubConnection con : connectionMap.values()) {
            if (con.lastActionFlush < minActiveTime) {
                inactiveCons.add(con);
            }
            else {
                activeCons.add(con);
            }
        }
        if (activeCons.isEmpty()) {
            activeCons.addAll(inactiveCons);
            inactiveCons.clear();
        }
        for (SubConnection con : inactiveCons) {
            con.setMaxInFlight(1);
        }
        int f = subscriber.getMaxInFlightPerSubscription() - inactiveCons.size();
        int perCon = f / activeCons.size();
        int extra = f % activeCons.size();
        for (SubConnection con : activeCons) {
            int c = perCon;
            if (extra > 0) {
                c++;
                extra--;
            }
            con.setMaxInFlight(Math.min(c, con.getMaxRdyCount()));
        }
    }

    private boolean checkLowFlight() {
        if (subscriber.getMaxInFlightPerSubscription() < connectionMap.size()) {
            if (lowFlightRotateTask == null) {
                lowFlightRotateTask = Client.scheduleAtFixedRate(new Runnable() {
                    public void run() {
                        rotateLowFlight();
                    }
                }, 10000, 10000);
            }
            List<SubConnection> cons = Lists.newArrayList(connectionMap.values());
            for (SubConnection con : cons.subList(0, subscriber.getMaxInFlightPerSubscription())) {
                con.setMaxInFlight(1);
            }
            for (SubConnection con : cons.subList(subscriber.getMaxInFlightPerSubscription(), cons.size())) {
                con.setMaxInFlight(0);
            }
            return true;
        }
        Util.cancel(lowFlightRotateTask);
        lowFlightRotateTask = null;
        return false;
    }

    private synchronized void rotateLowFlight() {
        SubConnection paused = null;
        SubConnection ready = null;
        for (SubConnection con : connectionMap.values()) {
            if (con.getMaxInFlight() == 0 && (paused == null || con.getLastActionFlush() < paused.getLastActionFlush())) {
                paused = con;
            }
            else if (con.getMaxInFlight() == 1 && (ready == null || con.getLastActionFlush() < ready.getLastActionFlush())) {
                ready = con;
            }
        }
        if (ready != null && paused != null) { //should always be true
            ready.setMaxInFlight(0);
            paused.setMaxInFlight(1);
        }
    }

    public synchronized void close() {
        Util.cancel(lowFlightRotateTask);
        lowFlightRotateTask = null;
        for (SubConnection con : connectionMap.values()) {
            con.close();
        }
        logger.debug("subscription closed:{}", topic);
    }

    @Subscribe
    public synchronized void connectionClosed(Connection closedCon) {
        if (connectionMap.remove(closedCon.getHost()) != null) {
            logger.debug("removed:{} from subscription:{}", closedCon.getHost(), topic);
        }
    }

    public synchronized boolean isLowFlight() {
        return lowFlightRotateTask != null;
    }

    public Subscriber getSubscriber() {
        return subscriber;
    }

    public MessageHandler getHandler() {
        return handler;
    }

    public String getTopic() {
        return topic;
    }

    public String getChannel() {
        return channel;
    }

}
