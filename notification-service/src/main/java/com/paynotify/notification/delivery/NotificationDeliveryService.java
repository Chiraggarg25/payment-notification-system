package com.paynotify.notification.delivery;

import com.paynotify.common.client.NonRetryableDeliveryException;
import com.paynotify.common.client.PlatformClient;
import com.paynotify.common.client.RetryableDeliveryException;
import com.paynotify.common.event.PaymentNotificationEvent;
import com.paynotify.common.store.DeliveredBy;
import com.paynotify.common.store.LockedRecord;
import com.paynotify.common.store.NotificationRecord;
import com.paynotify.common.store.NotificationStore;
import com.paynotify.notification.config.NotificationProperties;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Delivers one event to its platform:
 * <pre>
 *   lock(txn) -> dedupe -> Retry( CircuitBreaker( HTTP call ) ) -> record outcome in Redis -> unlock(txn)
 * </pre>
 * <ul>
 *   <li>Lock: the sweeper uses the same per-transaction lock, so the two can never send or overwrite the
 *       same transaction concurrently (no stale status overtaking a newer one).</li>
 *   <li>Idempotency: an eventId already processed is skipped; an event older than the one already recorded
 *       for the transaction is skipped as SUPERSEDED.</li>
 *   <li>Retry: max-attempts=4 with exponential backoff 1s, 2s, 4s (bounded). CircuitBreaker: per platform;
 *       when OPEN, calls fail fast with CallNotPermittedException (not retried).</li>
 * </ul>
 * Runs on the platform's Kafka consumer thread (a dedicated blocking thread), so the backoff sleeps and the
 * synchronous HTTP call are intentional - this is not async/event-loop code.
 */
@Service
public class NotificationDeliveryService {

    private static final Logger log = LoggerFactory.getLogger(NotificationDeliveryService.class);

    private final PlatformClient platformClient;
    private final NotificationStore store;
    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final RetryRegistry retryRegistry;
    private final NotificationProperties.Delivery config;

    public NotificationDeliveryService(PlatformClient platformClient,
                                       NotificationStore store,
                                       CircuitBreakerRegistry circuitBreakerRegistry,
                                       RetryRegistry retryRegistry,
                                       NotificationProperties properties) {
        this.platformClient = platformClient;
        this.store = store;
        this.circuitBreakerRegistry = circuitBreakerRegistry;
        this.retryRegistry = retryRegistry;
        this.config = properties.delivery();
    }

    public DeliveryOutcome deliver(PaymentNotificationEvent event) {
        String member = NotificationStore.member(event.platformId(), event.transactionId());
        LockedRecord locked = store.lockAndLoad(member, config.lockTtl(), config.lockWait(), config.lockPollInterval())
                .orElseThrow(() -> new DeliveryLockUnavailableException(member));
        try {
            return deliverLocked(event, locked.record());
        } finally {
            store.unlock(member, locked.token());
        }
    }

    private DeliveryOutcome deliverLocked(PaymentNotificationEvent event, Optional<NotificationRecord> existing) {
        if (store.isProcessed(event.eventId())) {
            log.info("[IDEMPOTENCY] event {} (txn {}) already handled -> skipping duplicate",
                    event.eventId(), event.transactionId());
            return DeliveryOutcome.DUPLICATE_SKIPPED;
        }
        if (existing.isPresent() && isSupersededBy(event, existing.get())) {
            log.info("[IDEMPOTENCY] event {} (txn {}, createdAt {}) is older than recorded event {} -> skipping",
                    event.eventId(), event.transactionId(), event.createdAt(), existing.get().eventId());
            store.markProcessed(event.eventId(), "SUPERSEDED");
            return DeliveryOutcome.SUPERSEDED;
        }

        String platform = event.platformId();
        CircuitBreaker circuitBreaker = circuitBreakerRegistry.circuitBreaker(platform);
        Retry retry = retryRegistry.retry(platform);
        AtomicInteger attempts = new AtomicInteger();

        Runnable httpCall = () -> {
            int attempt = attempts.incrementAndGet();
            log.info("[DELIVERY] {} txn={} status={} attempt #{} (circuit={})",
                    platform, event.transactionId(), event.paymentStatus(), attempt, circuitBreaker.getState());
            platformClient.send(event);
        };
        Runnable resilientCall = Retry.decorateRunnable(retry, CircuitBreaker.decorateRunnable(circuitBreaker, httpCall));

        try {
            resilientCall.run();
            store.markDelivered(event, attempts.get(), 0, DeliveredBy.CONSUMER);
            log.info("[DELIVERY] {} txn={} delivered after {} attempt(s)", platform, event.transactionId(), attempts.get());
            return DeliveryOutcome.DELIVERED;
        } catch (CallNotPermittedException e) {
            store.markRetryPending(event, attempts.get(),
                    "Circuit breaker OPEN for '" + platform + "' - call not permitted", nextRetryAt());
            return DeliveryOutcome.PARKED_FOR_RETRY;
        } catch (NonRetryableDeliveryException e) {
            store.markDead(event, attempts.get(), "Non-retryable: " + e.getMessage());
            return DeliveryOutcome.DEAD;
        } catch (RetryableDeliveryException e) {
            store.markRetryPending(event, attempts.get(),
                    "Retries exhausted (" + attempts.get() + " attempts): " + e.getMessage(), nextRetryAt());
            return DeliveryOutcome.PARKED_FOR_RETRY;
        }
        // Any other exception (e.g. Redis down while recording the outcome) propagates to the bounded
        // Kafka error handler: offset not committed, record re-processed, finally dead-lettered.
    }

    /** True if the transaction already holds a DIFFERENT event that was created after this one. */
    private static boolean isSupersededBy(PaymentNotificationEvent incoming, NotificationRecord recorded) {
        if (recorded.eventId() == null || recorded.eventId().equals(incoming.eventId()) || incoming.createdAt() == null) {
            return false;
        }
        return recorded.createdAtInstant().map(t -> t.isAfter(incoming.createdAt())).orElse(false);
    }

    private Instant nextRetryAt() {
        return Instant.now().plus(config.firstRetryDelay());
    }
}
