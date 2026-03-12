package com.sproutsocial.nsq;

/**
 * Error codes for NSQ exceptions
 */
public enum NSQErrorCode {
    /**
     * Authentication failure - auth session expired or invalid credentials
     */
    AUTH_FAILED,

    /**
     * General NSQ error (protocol errors, invalid operations, etc.)
     */
    GENERAL
}
