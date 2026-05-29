package com.sproutsocial.nsq;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit test (no Docker) for the log-level decision in {@link SubConnection#setMaxInFlight(int, boolean)}.
 * A "Socket closed" {@link IOException} during flush() is benign when the read thread has already torn
 * the connection down (isReading == false) and must not be logged at ERROR, since Sentry captures ERROR
 * logs as issues. A genuine failure while the connection still believes it is reading must stay at ERROR.
 */
public class SubConnectionTest {

    private static final String LOGGER_NAME = "com.sproutsocial.nsq.SubConnection";

    private ch.qos.logback.classic.Logger subConnLogger;
    private ListAppender<ILoggingEvent> appender;

    @Before
    public void setUp() {
        subConnLogger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(LOGGER_NAME);
        subConnLogger.setLevel(Level.DEBUG);
        appender = new ListAppender<ILoggingEvent>();
        appender.start();
        subConnLogger.addAppender(appender);
    }

    @After
    public void tearDown() {
        subConnLogger.detachAppender(appender);
    }

    @Test
    public void logsErrorWhenFlushFailsWhileStillReading() throws Exception {
        SubConnection con = newSubConnection();
        // isReading defaults to true: a flush failure here is a genuine, actionable error.

        con.setMaxInFlight(5, true);

        ILoggingEvent event = findSetMaxInFlightEvent();
        assertNotNull("expected a setMaxInFlight failure log event", event);
        assertEquals(Level.ERROR, event.getLevel());
        assertFalse("close() should have run, flipping isReading to false", getIsReading(con));
    }

    @Test
    public void logsDebugWhenFlushFailsAfterConnectionTornDown() throws Exception {
        SubConnection con = newSubConnection();
        // Simulate the read thread having already closed the socket: isReading == false.
        setIsReading(con, false);

        con.setMaxInFlight(5, true);

        ILoggingEvent event = findSetMaxInFlightEvent();
        assertNotNull("expected a setMaxInFlight failure log event", event);
        assertEquals(Level.DEBUG, event.getLevel());
        assertNull("no ERROR-level setMaxInFlight event should be logged for a benign teardown race",
                findSetMaxInFlightEventAtLevel(Level.ERROR));
    }

    private SubConnection newSubConnection() throws Exception {
        Client client = mock(Client.class);
        when(client.getExecutor()).thenReturn(mock(ExecutorService.class));
        when(client.getSchedExecutor()).thenReturn(mock(ScheduledExecutorService.class));
        when(client.scheduleAtFixedRate(any(Runnable.class), anyInt(), anyInt(), anyBoolean())).thenReturn(null);

        Subscriber subscriber = mock(Subscriber.class);
        when(subscriber.getMaxAttempts()).thenReturn(5);
        when(subscriber.getMaxFlushDelayMillis()).thenReturn(2000);
        when(subscriber.getFailedMessageHandler()).thenReturn(null);

        Subscription subscription = mock(Subscription.class);
        when(subscription.getSubscriber()).thenReturn(subscriber);
        when(subscription.getHandler()).thenReturn(null);
        when(subscription.getTopic()).thenReturn("topic");

        SubConnection con = new SubConnection(client, new HostAndPort("host", 1), subscription);

        // Replace the output stream so writeCommand buffers without error but flush() throws,
        // exactly reproducing a socket that was closed underneath setMaxInFlight.
        OutputStream throwingOnFlush = new OutputStream() {
            @Override
            public void write(int b) {
                // no-op: commands buffer fine
            }

            @Override
            public void flush() throws IOException {
                throw new IOException("Socket closed");
            }
        };
        setOut(con, new DataOutputStream(throwingOnFlush));
        return con;
    }

    private ILoggingEvent findSetMaxInFlightEvent() {
        for (ILoggingEvent event : appender.list) {
            if (event.getFormattedMessage().startsWith("setMaxInFlight failed")) {
                return event;
            }
        }
        return null;
    }

    private ILoggingEvent findSetMaxInFlightEventAtLevel(Level level) {
        for (ILoggingEvent event : appender.list) {
            if (event.getLevel() == level && event.getFormattedMessage().startsWith("setMaxInFlight failed")) {
                return event;
            }
        }
        return null;
    }

    private static void setOut(SubConnection con, DataOutputStream out) throws Exception {
        Field field = Connection.class.getDeclaredField("out");
        field.setAccessible(true);
        field.set(con, out);
    }

    private static void setIsReading(SubConnection con, boolean value) throws Exception {
        Field field = Connection.class.getDeclaredField("isReading");
        field.setAccessible(true);
        field.setBoolean(con, value);
    }

    private static boolean getIsReading(SubConnection con) throws Exception {
        Field field = Connection.class.getDeclaredField("isReading");
        field.setAccessible(true);
        return field.getBoolean(con);
    }
}
