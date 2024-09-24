package com.sproutsocial.nsq;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class NSQMessageTest {

    private final SubConnection connection = Mockito.mock(SubConnection.class);

    @Before
    public void resetTest() {
        Mockito.reset(connection);
    }

    @Test
    public void shouldMarkRespondedOnFinish() {
        final NSQMessage message = new NSQMessage(0L, 0, "id", null, "topic", connection);
        assertFalse(message.hasResponded());
        message.finish();
        assertTrue(message.hasResponded());
        Mockito.verify(connection).finish("id");
        Mockito.verifyNoMoreInteractions(connection);
    }

    @Test
    public void shouldMarkRespondedOnRequeue() {
        final NSQMessage message = new NSQMessage(0L, 0, "id", null, "topic", connection);
        assertFalse(message.hasResponded());
        message.requeue();
        assertTrue(message.hasResponded());
        Mockito.verify(connection).requeue("id", 0);
        Mockito.verifyNoMoreInteractions(connection);
    }

    @Test
    public void shouldNotTouchWhenResponded() {
        final NSQMessage message = new NSQMessage(0L, 0, "id", null, "topic", connection);
        assertFalse(message.hasResponded());
        message.touch();
        Mockito.verify(connection).touch("id");
        message.requeue();
        assertTrue(message.hasResponded());
        // reset mock
        Mockito.reset(connection);
        message.touch();
        Mockito.verifyZeroInteractions(connection);
    }

}