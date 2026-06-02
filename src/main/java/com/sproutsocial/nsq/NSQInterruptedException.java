package com.sproutsocial.nsq;

public class NSQInterruptedException extends NSQException {
    public NSQInterruptedException(String message) {
        super(message);
    }

    public NSQInterruptedException(String message, Throwable cause) {
        super(message, cause);
    }
}
