package com.sproutsocial.nsq;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledFuture;

class BasePubSub {

    protected Config config = new Config();
    protected volatile boolean isStopping = false;
    protected final Client client;
    private final List<ScheduledFuture> tasks = Collections.synchronizedList(new ArrayList<ScheduledFuture>());

    protected BasePubSub(Client client) {
        this.client = client;
    }

    public final Client getClient() {
        return client;
    }

    public synchronized Config getConfig() {
        return config;
    }

    public synchronized void setConfig(Config config) {
        this.config = config;
    }

    protected void scheduleAtFixedRate(Runnable runnable, int initialDelay, int period, boolean jitter) {
        if (!isStopping) {
            tasks.add(client.scheduleAtFixedRate(runnable, initialDelay, period, jitter));
        }
    }

    public void stop() {
        isStopping = true;
        cancelTasks();
    }

    protected void cancelTasks() {
        for (ScheduledFuture task : tasks) {
            task.cancel(false);
        }
        tasks.clear();
    }

}
