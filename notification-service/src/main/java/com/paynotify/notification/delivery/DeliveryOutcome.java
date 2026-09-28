package com.paynotify.notification.delivery;

public enum DeliveryOutcome {
    DELIVERED,
    /** Same eventId already handled (Kafka re-delivery / client retry) - nothing sent. */
    DUPLICATE_SKIPPED,
    /** An older event than the one already recorded for this transaction (e.g. offset replay) - nothing sent. */
    SUPERSEDED,
    PARKED_FOR_RETRY,
    DEAD
}
