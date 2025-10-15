package com.sproutsocial.nsq;

public class NSQException extends RuntimeException {

    private final NSQErrorCode errorCode;

    public NSQException(String message) {
        this(message, NSQErrorCode.GENERAL);
    }

    public NSQException(String message, NSQErrorCode errorCode) {
        super(message);
        this.errorCode = errorCode;
    }

    public NSQException(String message, Throwable cause) {
        this(message, NSQErrorCode.GENERAL, cause);
    }

    public NSQException(String message, NSQErrorCode errorCode, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public NSQErrorCode getErrorCode() {
        return errorCode;
    }

}
