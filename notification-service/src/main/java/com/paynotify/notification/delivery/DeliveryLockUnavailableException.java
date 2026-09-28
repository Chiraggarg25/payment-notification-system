package com.paynotify.notification.delivery;

/**
 * The sweeper is sending this transaction right now and did not release the lock within app.delivery.lock-wait.
 * Thrown to the Kafka error handler, which re-processes the record with bounded backoff (offset not committed).
 */
public class DeliveryLockUnavailableException extends RuntimeException {
    public DeliveryLockUnavailableException(String member) {
        super("Transaction lock for '" + member + "' is held by another worker");
    }
}
