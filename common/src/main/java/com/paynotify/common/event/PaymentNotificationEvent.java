package com.paynotify.common.event;

import java.time.Instant;

/**
 * Event published by platform-service to Kafka and consumed by notification-service.
 *
 * @param eventId       unique id of this event (used as Idempotency-Key towards the platform and for de-duplication)
 * @param platformId    target platform (udemy, amazon, coursera ...). Also the Kafka message key.
 * @param transactionId transaction the payment status belongs to
 * @param paymentStatus status to deliver
 * @param createdAt     time the event was accepted by platform-service
 */
public record PaymentNotificationEvent(
        String eventId,
        String platformId,
        String transactionId,
        PaymentStatus paymentStatus,
        Instant createdAt) {
}
