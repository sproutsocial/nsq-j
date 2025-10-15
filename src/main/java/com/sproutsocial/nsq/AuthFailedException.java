package com.sproutsocial.nsq;

/**
 * Exception thrown when NSQ server returns E_AUTH_FAILED or E_UNAUTHORIZED errors.
 * This typically indicates that the server-side auth session has expired while the
 * TCP connection remained open.
 */
public class AuthFailedException extends NSQException {

    public AuthFailedException(String message) {
        super(message);
    }

    public AuthFailedException(String message, Throwable cause) {
        super(message, cause);
    }

}
