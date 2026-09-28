package com.paynotify.sweeper.job;

import com.paynotify.common.client.NonRetryableDeliveryException;
import com.paynotify.common.client.PlatformClient;
import com.paynotify.common.client.PlatformProperties;
import com.paynotify.common.event.PaymentNotificationEvent;
import com.paynotify.common.store.DeliveredBy;
import com.paynotify.common.store.DeliveryStatus;
import com.paynotify.common.store.LockedRecord;
import com.paynotify.common.store.NotificationRecord;
import com.paynotify.common.store.NotificationStore;
import com.paynotify.sweeper.config.SweeperProperties;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Periodically picks due items from the Redis retry queue (ZSET scored by nextRetryAt) and re-sends them.
 * <ul>
 *   <li>success                 -> DELIVERED (removed from the queue)</li>
 *   <li>transient failure       -> sweepAttempts++, re-scheduled with exponential backoff (10s, 20s, 40s, 80s ...)</li>
 *   <li>sweepAttempts == max    -> DEAD (removed from queue, added to notifications:dead)</li>
 *   <li>4xx / corrupt record    -> DEAD immediately</li>
 *   <li>circuit OPEN            -> postponed without consuming an attempt; bounded by max-postpones</li>
 *   <li>consumer holds the lock -> re-scheduled a few seconds later (the consumer is handling it)</li>
 * </ul>
 * Concurrency: items are claimed atomically (Lua, with a lease) and every item is processed while holding
 * its per-transaction lock, so two sweeps (scheduled + manual, or several instances) and the consumer can
 * never send the same transaction at the same time. Platforms are swept in parallel on a bounded pool
 * with an overall timeout.
 */
@Component
public class FailedNotificationSweeper {

    private static final Logger log = LoggerFactory.getLogger(FailedNotificationSweeper.class);

    private enum ItemOutcome { DELIVERED, RESCHEDULED, POSTPONED, DEAD, SKIPPED, BUSY, ERROR }

    private final NotificationStore store;
    private final PlatformClient platformClient;
    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final SweeperProperties props;
    private final ExecutorService executor;

    public FailedNotificationSweeper(NotificationStore store,
                                     PlatformClient platformClient,
                                     CircuitBreakerRegistry circuitBreakerRegistry,
                                     SweeperProperties props,
                                     ExecutorService sweeperExecutor,
                                     PlatformProperties platforms) {
        Duration oneCall = platforms.getConnectTimeout().plus(platforms.getReadTimeout());
        if (props.lockTtl().compareTo(oneCall) <= 0) {
            // otherwise the lock could expire while the HTTP call is still in flight
            throw new IllegalStateException("app.sweeper.lock-ttl (" + props.lockTtl()
                    + ") must be greater than platforms.connect-timeout + read-timeout (" + oneCall + ")");
        }
        this.store = store;
        this.platformClient = platformClient;
        this.circuitBreakerRegistry = circuitBreakerRegistry;
        this.props = props;
        this.executor = sweeperExecutor;
    }

    @Scheduled(fixedDelayString = "${app.sweeper.poll-interval-ms:5000}",
               initialDelayString = "${app.sweeper.initial-delay-ms:10000}")
    public void scheduledSweep() {
        if (!props.enabled()) {
            return;
        }
        try {
            sweepOnce();
        } catch (RuntimeException e) {
            // e.g. Redis unavailable - just try again on the next tick
            log.error("[SWEEPER] sweep failed: {}", e.getMessage());
        }
    }

    public SweepResult sweepOnce() {
        List<String> members = store.claimDue(Instant.now(), props.batchSize(), props.claimLease());
        if (members.isEmpty()) {
            return SweepResult.empty();
        }
        log.info("[SWEEPER] claimed {} due notification(s): {}", members.size(), members);

        // one task per platform: sequential within a platform, platforms in parallel
        Map<String, List<String>> byPlatform = new LinkedHashMap<>();
        for (String member : members) {
            byPlatform.computeIfAbsent(platformOf(member), k -> new ArrayList<>()).add(member);
        }
        List<Callable<Map<ItemOutcome, Integer>>> tasks = new ArrayList<>();
        byPlatform.values().forEach(batch -> tasks.add(() -> processBatch(batch)));

        Map<ItemOutcome, Integer> totals = new EnumMap<>(ItemOutcome.class);
        int timedOut = 0;
        try {
            List<Future<Map<ItemOutcome, Integer>>> futures =
                    executor.invokeAll(tasks, props.sweepTimeout().toMillis(), TimeUnit.MILLISECONDS);
            for (Future<Map<ItemOutcome, Integer>> future : futures) {
                try {
                    future.get().forEach((k, v) -> totals.merge(k, v, Integer::sum));
                } catch (CancellationException e) {
                    timedOut++;   // unfinished items keep their claim lease and come back later
                } catch (ExecutionException e) {
                    log.error("[SWEEPER] platform batch failed: {}", e.getCause() == null ? e : e.getCause().getMessage());
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("[SWEEPER] interrupted - remaining items come back after the claim lease");
        }

        SweepResult result = new SweepResult(members.size(),
                totals.getOrDefault(ItemOutcome.DELIVERED, 0),
                totals.getOrDefault(ItemOutcome.RESCHEDULED, 0),
                totals.getOrDefault(ItemOutcome.POSTPONED, 0),
                totals.getOrDefault(ItemOutcome.DEAD, 0),
                totals.getOrDefault(ItemOutcome.SKIPPED, 0),
                totals.getOrDefault(ItemOutcome.BUSY, 0),
                totals.getOrDefault(ItemOutcome.ERROR, 0),
                timedOut);
        log.info("[SWEEPER] done: {}", result);
        return result;
    }

    private Map<ItemOutcome, Integer> processBatch(List<String> batch) {
        Map<ItemOutcome, Integer> counts = new EnumMap<>(ItemOutcome.class);
        for (String member : batch) {
            if (Thread.currentThread().isInterrupted()) {
                break;   // sweep timeout: stop cleanly, remaining items stay claimed until the lease expires
            }
            ItemOutcome outcome;
            try {
                outcome = process(member);
            } catch (RuntimeException e) {
                // one bad item (e.g. transient Redis error) must not abort the rest of the batch
                log.error("[SWEEPER] {} failed unexpectedly: {}", member, e.getMessage());
                outcome = ItemOutcome.ERROR;
            }
            counts.merge(outcome, 1, Integer::sum);
        }
        return counts;
    }

    private ItemOutcome process(String member) {
        Optional<LockedRecord> locked = store.tryLockAndLoad(member, props.lockTtl());
        if (locked.isEmpty()) {
            log.info("[SWEEPER] {} is being handled by the consumer right now -> retry in {}", member, props.lockBusyDelay());
            store.reschedule(member, Instant.now().plus(props.lockBusyDelay()));
            return ItemOutcome.BUSY;
        }
        try {
            return processLocked(member, locked.get().record());
        } finally {
            store.unlock(member, locked.get().token());
        }
    }

    private ItemOutcome processLocked(String member, Optional<NotificationRecord> maybeRecord) {
        if (maybeRecord.isEmpty() || maybeRecord.get().deliveryStatus() != DeliveryStatus.RETRY_PENDING) {
            store.removeFromQueue(member);   // stale queue entry (already delivered / dead / deleted)
            return ItemOutcome.SKIPPED;
        }
        NotificationRecord record = maybeRecord.get();
        Optional<PaymentNotificationEvent> maybeEvent = record.toEvent();
        if (maybeEvent.isEmpty()) {
            store.markDead(member, record.sweepAttempts(), "Corrupt record in Redis - cannot rebuild the event");
            return ItemOutcome.DEAD;
        }
        PaymentNotificationEvent event = maybeEvent.get();
        CircuitBreaker circuitBreaker = circuitBreakerRegistry.circuitBreaker(event.platformId());
        int attempt = record.sweepAttempts() + 1;

        log.info("[SWEEPER] {} sweep attempt #{}/{} (circuit={})", member, attempt, props.maxAttempts(),
                circuitBreaker.getState());
        try {
            circuitBreaker.executeRunnable(() -> platformClient.send(event));
        } catch (CallNotPermittedException e) {
            int postpones = record.postpones() + 1;
            if (postpones > props.maxPostpones()) {
                store.markDead(member, record.sweepAttempts(),
                        "Circuit breaker for '" + event.platformId() + "' stayed OPEN for " + props.maxPostpones() + " postpones");
                return ItemOutcome.DEAD;
            }
            store.postpone(member, postpones, "Sweeper circuit breaker OPEN for '" + event.platformId() + "'",
                    Instant.now().plus(openStateWait(circuitBreaker)));
            return ItemOutcome.POSTPONED;
        } catch (NonRetryableDeliveryException e) {
            store.markDead(member, attempt, "Non-retryable: " + e.getMessage());
            return ItemOutcome.DEAD;
        } catch (RuntimeException e) {
            String error = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
            if (attempt >= props.maxAttempts()) {
                store.markDead(member, attempt,
                        "Max sweep attempts (" + props.maxAttempts() + ") exhausted. Last error: " + error);
                return ItemOutcome.DEAD;
            }
            store.recordSweepFailure(member, attempt, error, Instant.now().plus(props.backoffFor(attempt)));
            return ItemOutcome.RESCHEDULED;
        }
        // outside the try: a Redis error here must not be mistaken for a failed delivery
        store.markDelivered(event, record.consumerAttempts(), attempt, DeliveredBy.SWEEPER);
        return ItemOutcome.DELIVERED;
    }

    private Duration openStateWait(CircuitBreaker circuitBreaker) {
        Long waitMs = circuitBreaker.getCircuitBreakerConfig().getWaitIntervalFunctionInOpenState().apply(1);
        return waitMs == null || waitMs <= 0 ? props.lockBusyDelay() : Duration.ofMillis(waitMs);
    }

    private static String platformOf(String member) {
        int idx = member.indexOf(':');
        return idx > 0 ? member.substring(0, idx) : member;
    }
}
