package com.paynotify.common.client;

/**
 * Permanent failure (4xx other than 408/429, unknown platform). Retrying will not help, so the
 * notification is parked as DEAD straight away and the circuit breaker ignores it.
 */
public class NonRetryableDeliveryException extends DeliveryException {
    public NonRetryableDeliveryException(String message, Throwable cause) {
        super(message, cause);
    }
}
