package com.paynotify.platform.api;

import com.paynotify.common.client.PlatformProperties;
import com.paynotify.common.event.PaymentNotificationEvent;
import com.paynotify.platform.config.PlatformServiceProperties;
import com.paynotify.platform.kafka.PaymentEventPublisher;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/api/v1")
public class PaymentNotificationController {

    private final PaymentEventPublisher publisher;
    private final PlatformProperties platforms;
    private final PlatformServiceProperties props;

    public PaymentNotificationController(PaymentEventPublisher publisher,
                                         PlatformProperties platforms,
                                         PlatformServiceProperties props) {
        this.publisher = publisher;
        this.platforms = platforms;
        this.props = props;
    }

    /**
     * Idempotent: the eventId is the client's {@code Idempotency-Key} header or, when absent, a deterministic
     * UUID of (platformId, transactionId, paymentStatus). A retried POST therefore produces the SAME eventId,
     * which the consumer de-duplicates and the platform receives as its Idempotency-Key.
     * <p>
     * Returns a CompletableFuture: the servlet thread is released while waiting for the Kafka ack.
     */
    @PostMapping("/payments/notifications")
    public CompletableFuture<ResponseEntity<PublishResponse>> publish(
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @Valid @RequestBody PaymentNotificationRequest request) {

        String platformId = PlatformProperties.normalize(request.platformId());
        if (!platforms.isSupported(platformId)) {
            throw new UnsupportedPlatformException(request.platformId());
        }
        String transactionId = request.transactionId().trim();
        String eventId = resolveEventId(idempotencyKey, platformId, transactionId, request.paymentStatus().name());

        PaymentNotificationEvent event = new PaymentNotificationEvent(
                eventId, platformId, transactionId, request.paymentStatus(), Instant.now());

        return publisher.publish(event).thenApply(md -> ResponseEntity.status(HttpStatus.ACCEPTED).body(
                new PublishResponse(event.eventId(), platformId, transactionId, event.paymentStatus().name(),
                        md.topic(), platformId, md.partition(), md.offset())));
    }

    @GetMapping("/platforms")
    public List<String> supportedPlatforms() {
        return platforms.platformIds();
    }

    private String resolveEventId(String idempotencyKey, String platformId, String transactionId, String status) {
        if (idempotencyKey != null && !idempotencyKey.isBlank()) {
            String key = idempotencyKey.trim();
            if (key.length() > props.maxIdempotencyKeyLength()) {
                throw new InvalidIdempotencyKeyException(
                        "Idempotency-Key longer than " + props.maxIdempotencyKeyLength() + " characters");
            }
            return key;
        }
        String natural = platformId + "|" + transactionId + "|" + status;
        return UUID.nameUUIDFromBytes(natural.getBytes(StandardCharsets.UTF_8)).toString();
    }
}
