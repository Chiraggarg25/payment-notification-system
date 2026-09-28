package com.paynotify.platform.api;

public record PublishResponse(
        String eventId,
        String platformId,
        String transactionId,
        String paymentStatus,
        String topic,
        String messageKey,
        int partition,
        long offset) {
}
