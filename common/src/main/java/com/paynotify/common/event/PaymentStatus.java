package com.paynotify.common.event;

/** Payment status of a transaction that the platform must be informed about. */
public enum PaymentStatus {
    SUCCESS,
    FAILED,
    PENDING,
    REFUNDED
}
