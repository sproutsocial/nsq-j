package com.sproutsocial.nsq;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SubscriberTest {

    @Test
    public void lookupTimeoutMillisDefaultsTo30000() {
        Subscriber subscriber = new Subscriber("localhost:4161");
        assertEquals(30000, subscriber.getLookupTimeoutMillis());
    }

    @Test
    public void setLookupTimeoutMillisUpdatesValue() {
        Subscriber subscriber = new Subscriber("localhost:4161");
        subscriber.setLookupTimeoutMillis(5000);
        assertEquals(5000, subscriber.getLookupTimeoutMillis());
    }

    @Test(expected = IllegalArgumentException.class)
    public void setLookupTimeoutMillisRejectsZero() {
        Subscriber subscriber = new Subscriber("localhost:4161");
        subscriber.setLookupTimeoutMillis(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void setLookupTimeoutMillisRejectsNegative() {
        Subscriber subscriber = new Subscriber("localhost:4161");
        subscriber.setLookupTimeoutMillis(-1);
    }
}
