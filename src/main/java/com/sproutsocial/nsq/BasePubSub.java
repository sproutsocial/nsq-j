package com.sproutsocial.nsq;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;

class BasePubSub {

    protected Config config = new Config();
    protected List<ScheduledFuture> tasks = new ArrayList<ScheduledFuture>();
    protected volatile boolean isStopping = false;

    public synchronized Config getConfig() {
        return config;
    }

    public synchronized void setConfig(Config config) {
        this.config = config;
    }

    protected void scheduleAtFixedRate(Runnable runnable, int initialDelay, int period, boolean jitter) {
        if (!isStopping) {
            tasks.add(Client.scheduleAtFixedRate(runnable, initialDelay, period, jitter));
        }
    }

    public void stop() {
        isStopping = true;
        for (ScheduledFuture task : tasks) {
            task.cancel(false);
        }
        tasks.clear();
    }

}
