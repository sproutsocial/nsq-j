package com.sproutsocial.nsq;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Represents a unique subscription that's returned from a call to {@link Subscriber#subscribe}.
 * Can be passed to methods such as {@link Subscriber#unsubscribe} to remove a subscription.
 */
public class SubscriptionId {
    private final long id;

    protected SubscriptionId(final long id) {
        this.id = id;
    }

    static SubscriptionId fromCounter(final AtomicLong counter) {
        return new SubscriptionId(counter.getAndIncrement());
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) { return true; }
        if (!(other instanceof SubscriptionId)) { return false; }
        SubscriptionId that = (SubscriptionId)other;
        return id == that.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "SubscriptionId { " + id + " }";
    }
}
