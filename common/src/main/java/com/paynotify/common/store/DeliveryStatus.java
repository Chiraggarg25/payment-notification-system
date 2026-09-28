package com.paynotify.common.store;

public enum DeliveryStatus {
    /** Platform acknowledged the notification. */
    DELIVERED,
    /** Consumer retries / circuit breaker exhausted; waiting for the sweeper to try again. */
    RETRY_PENDING,
    /** Sweeper attempts maxed out (or permanent 4xx). Needs manual action (requeue). */
    DEAD
}
