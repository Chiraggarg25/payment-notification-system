package com.paynotify.common.store;

import com.paynotify.common.event.PaymentNotificationEvent;
import com.paynotify.common.event.PaymentStatus;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.Optional;

/**
 * Snapshot of a notification as stored in the Redis hash {@code notification:{platformId}:{transactionId}}.
 * Parsing is defensive: a missing or corrupt field never throws, it becomes null / 0 and
 * {@link #toEvent()} returns empty when the record cannot be turned back into a valid event.
 */
public record NotificationRecord(
        String eventId,
        String platformId,
        String transactionId,
        String paymentStatus,
        DeliveryStatus deliveryStatus,
        int consumerAttempts,
        int sweepAttempts,
        int postpones,
        String lastError,
        String createdAt,
        String firstFailedAt,
        String lastAttemptAt,
        String nextRetryAt,
        String deliveredAt,
        String deliveredBy,
        String updatedAt) {

    static NotificationRecord fromHash(Map<String, String> h) {
        return new NotificationRecord(
                h.get(F.EVENT_ID),
                h.get(F.PLATFORM_ID),
                h.get(F.TRANSACTION_ID),
                h.get(F.PAYMENT_STATUS),
                parseEnum(DeliveryStatus.class, h.get(F.DELIVERY_STATUS)),
                parseInt(h.get(F.CONSUMER_ATTEMPTS)),
                parseInt(h.get(F.SWEEP_ATTEMPTS)),
                parseInt(h.get(F.POSTPONES)),
                h.get(F.LAST_ERROR),
                h.get(F.CREATED_AT),
                h.get(F.FIRST_FAILED_AT),
                h.get(F.LAST_ATTEMPT_AT),
                h.get(F.NEXT_RETRY_AT),
                h.get(F.DELIVERED_AT),
                h.get(F.DELIVERED_BY),
                h.get(F.UPDATED_AT));
    }

    /** @return the event to re-send, or empty if mandatory fields are missing / corrupt. */
    public Optional<PaymentNotificationEvent> toEvent() {
        PaymentStatus status = parseEnum(PaymentStatus.class, paymentStatus);
        if (isBlank(eventId) || isBlank(platformId) || isBlank(transactionId) || status == null) {
            return Optional.empty();
        }
        return Optional.of(new PaymentNotificationEvent(eventId, platformId, transactionId, status,
                createdAtInstant().orElse(null)));
    }

    public Optional<Instant> createdAtInstant() {
        return parseInstant(createdAt);
    }

    static Optional<Instant> parseInstant(String value) {
        if (isBlank(value)) {
            return Optional.empty();
        }
        try {
            return Optional.of(Instant.parse(value));
        } catch (DateTimeParseException e) {
            return Optional.empty();
        }
    }

    private static int parseInt(String v) {
        if (isBlank(v)) {
            return 0;
        }
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private static <E extends Enum<E>> E parseEnum(Class<E> type, String v) {
        if (isBlank(v)) {
            return null;
        }
        try {
            return Enum.valueOf(type, v.trim());
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }

    /** Hash field names. */
    static final class F {
        static final String EVENT_ID = "eventId";
        static final String PLATFORM_ID = "platformId";
        static final String TRANSACTION_ID = "transactionId";
        static final String PAYMENT_STATUS = "paymentStatus";
        static final String DELIVERY_STATUS = "deliveryStatus";
        static final String CONSUMER_ATTEMPTS = "consumerAttempts";
        static final String SWEEP_ATTEMPTS = "sweepAttempts";
        static final String POSTPONES = "postpones";
        static final String LAST_ERROR = "lastError";
        static final String CREATED_AT = "createdAt";
        static final String FIRST_FAILED_AT = "firstFailedAt";
        static final String LAST_ATTEMPT_AT = "lastAttemptAt";
        static final String NEXT_RETRY_AT = "nextRetryAt";
        static final String DELIVERED_AT = "deliveredAt";
        static final String DELIVERED_BY = "deliveredBy";
        static final String UPDATED_AT = "updatedAt";

        private F() {
        }
    }
}
