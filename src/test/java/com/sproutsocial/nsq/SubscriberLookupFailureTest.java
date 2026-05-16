package com.sproutsocial.nsq;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class SubscriberLookupFailureTest {

    private static final int MAX_FAILURES_BEFORE_ERROR = 3;
    private static final String UNREACHABLE_HOST = "127.0.0.1:1";

    private Subscriber subscriber;
    private ListAppender<ILoggingEvent> logAppender;
    private Logger subscriberLogger;

    @Before
    public void setUp() {
        subscriber = new Subscriber(Client.getDefaultClient(), 60, MAX_FAILURES_BEFORE_ERROR, UNREACHABLE_HOST);

        subscriberLogger = (Logger) LoggerFactory.getLogger(Subscriber.class);
        logAppender = new ListAppender<>();
        logAppender.start();
        subscriberLogger.addAppender(logAppender);
    }

    @After
    public void tearDown() {
        subscriberLogger.detachAppender(logAppender);
        subscriber.stop();
    }

    @Test
    public void errorLoggedExactlyOnceAtThreshold() {
        int totalCalls = 6;
        for (int i = 0; i < totalCalls; i++) {
            subscriber.lookupTopic("test-topic");
        }

        List<ILoggingEvent> lookupEvents = logAppender.list.stream()
                .filter(e -> e.getMessage().contains("lookup failure"))
                .collect(Collectors.toList());

        assertEquals(totalCalls, lookupEvents.size());

        long errorCount = lookupEvents.stream().filter(e -> e.getLevel() == Level.ERROR).count();
        long warnCount = lookupEvents.stream().filter(e -> e.getLevel() == Level.WARN).count();

        assertEquals("ERROR should be logged exactly once at the threshold", 1, errorCount);
        assertEquals("All other failures should be WARN", totalCalls - 1, warnCount);

        assertEquals(Level.WARN, lookupEvents.get(0).getLevel());
        assertEquals(Level.WARN, lookupEvents.get(1).getLevel());
        assertEquals(Level.ERROR, lookupEvents.get(2).getLevel());
        assertEquals(Level.WARN, lookupEvents.get(3).getLevel());
        assertEquals(Level.WARN, lookupEvents.get(4).getLevel());
        assertEquals(Level.WARN, lookupEvents.get(5).getLevel());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void errorRelogsAfterRecoveryAndReFailure() throws Exception {
        for (int i = 0; i < MAX_FAILURES_BEFORE_ERROR; i++) {
            subscriber.lookupTopic("test-topic");
        }

        Field failuresField = Subscriber.class.getDeclaredField("failures");
        failuresField.setAccessible(true);
        ((Map<String, Integer>) failuresField.get(subscriber)).clear();

        for (int i = 0; i < MAX_FAILURES_BEFORE_ERROR; i++) {
            subscriber.lookupTopic("test-topic");
        }

        List<ILoggingEvent> lookupEvents = logAppender.list.stream()
                .filter(e -> e.getMessage().contains("lookup failure"))
                .collect(Collectors.toList());

        long errorCount = lookupEvents.stream().filter(e -> e.getLevel() == Level.ERROR).count();
        assertEquals("ERROR should be logged once per threshold crossing", 2, errorCount);
    }
}
