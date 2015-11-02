package com.sproutsocial.nsq;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.google.common.eventbus.EventBus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

class Client {

    protected Config config = new Config();

    private static final ScheduledExecutorService schedExecutor = Executors.newSingleThreadScheduledExecutor(Util.threadFactory("nsq-sched"));
    public static final ObjectMapper mapper = new ObjectMapper();
    public static final EventBus eventBus = new EventBus();

    private static final Logger logger = LoggerFactory.getLogger(Client.class);

    static {
        mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        mapper.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);
    }

    public synchronized Config getConfig() {
        return config;
    }

    public synchronized void setConfig(Config config) {
        this.config = config;
    }

    public static long clock() {
        return System.currentTimeMillis();
    }

    public static ScheduledFuture scheduleAtFixedRate(final Runnable runnable, int initialDelay, int period) {
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

}
