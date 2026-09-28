package com.paynotify.common.client;

/** Base type for failures while delivering a notification to a platform. */
public abstract class DeliveryException extends RuntimeException {
    protected DeliveryException(String message, Throwable cause) {
        super(message, cause);
    }
}
