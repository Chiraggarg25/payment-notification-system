package com.paynotify.mockvendor;

public enum VendorMode {
    /** 200 OK */
    UP,
    /** 503 Service Unavailable - retryable, trips the circuit breaker */
    DOWN,
    /** 50% of the calls return 500 */
    FLAKY,
    /** sleeps 5s - longer than the client's 2s read timeout */
    TIMEOUT,
    /** 400 Bad Request - non-retryable, goes straight to DEAD */
    REJECT
}
