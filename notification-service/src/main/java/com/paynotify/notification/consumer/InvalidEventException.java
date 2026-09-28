package com.paynotify.notification.consumer;

/**
 * The Kafka record can never be processed (empty, not JSON, missing fields, wrong topic).
 * Registered as NOT retryable: the error handler publishes it straight to the dead-letter topic.
 */
public class InvalidEventException extends RuntimeException {
    public InvalidEventException(String message, Throwable cause) {
        super(message, cause);
    }
}
