package com.paynotify.common.client;

/**
 * Transient failure (platform down, 5xx, 408/429, timeout, connection refused).
 * Retried with exponential backoff and counted as a failure by the circuit breaker.
 */
public class RetryableDeliveryException extends DeliveryException {
    public RetryableDeliveryException(String message, Throwable cause) {
        super(message, cause);
    }
}
