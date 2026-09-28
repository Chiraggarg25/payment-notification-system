package com.paynotify.sweeper.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/**
 * All sweeper tunables (application.yml -> app.sweeper.*), validated at startup.
 *
 * @param enabled             turn the scheduled sweep on/off (manual sweep endpoint always works)
 * @param batchSize           max items claimed per sweep
 * @param maxAttempts         real send attempts by the sweeper before the notification is marked DEAD
 * @param baseDelay           backoff after the 1st failed sweep attempt
 * @param multiplier          exponential multiplier (baseDelay * multiplier^(attempt-1))
 * @param maxDelay            cap for the backoff
 * @param maxPostpones        times an item may be postponed because the circuit is OPEN before it is DEAD
 *                            (postpones don't consume attempts, so this bounds them separately)
 * @param claimLease          how long a claimed item is hidden from other sweeper runs/instances
 * @param lockTtl             per-transaction lock lease while sending (> connect + read timeout)
 * @param lockBusyDelay       retry delay when the consumer currently holds the transaction lock
 * @param platformParallelism threads used to sweep different platforms concurrently
 * @param sweepTimeout        max wall-clock time of one sweep; must be < claimLease
 * @param maxListLimit        upper bound for the list endpoint's ?limit=
 */
@ConfigurationProperties(prefix = "app.sweeper")
public record SweeperProperties(
        @DefaultValue("true") boolean enabled,
        @DefaultValue("50") int batchSize,
        @DefaultValue("5") int maxAttempts,
        @DefaultValue("10s") Duration baseDelay,
        @DefaultValue("2.0") double multiplier,
        @DefaultValue("10m") Duration maxDelay,
        @DefaultValue("20") int maxPostpones,
        @DefaultValue("60s") Duration claimLease,
        @DefaultValue("30s") Duration lockTtl,
        @DefaultValue("5s") Duration lockBusyDelay,
        @DefaultValue("4") int platformParallelism,
        @DefaultValue("45s") Duration sweepTimeout,
        @DefaultValue("1000") int maxListLimit) {

    public SweeperProperties {
        positive(batchSize, "batch-size");
        positive(maxAttempts, "max-attempts");
        positive(maxPostpones, "max-postpones");
        positive(platformParallelism, "platform-parallelism");
        positive(maxListLimit, "max-list-limit");
        positive(baseDelay, "base-delay");
        positive(maxDelay, "max-delay");
        positive(claimLease, "claim-lease");
        positive(lockTtl, "lock-ttl");
        positive(lockBusyDelay, "lock-busy-delay");
        positive(sweepTimeout, "sweep-timeout");
        if (multiplier < 1.0) {
            throw new IllegalArgumentException("app.sweeper.multiplier must be >= 1.0");
        }
        if (sweepTimeout.compareTo(claimLease) >= 0) {
            throw new IllegalArgumentException("app.sweeper.sweep-timeout must be < app.sweeper.claim-lease");
        }
    }

    /** baseDelay * multiplier^(attempt-1), capped at maxDelay (overflow-safe). */
    public Duration backoffFor(int attempt) {
        double factor = Math.pow(multiplier, Math.max(0, attempt - 1));
        double millis = Math.min(baseDelay.toMillis() * factor, (double) maxDelay.toMillis());
        return Duration.ofMillis((long) millis);
    }

    private static void positive(int v, String name) {
        if (v <= 0) {
            throw new IllegalArgumentException("app.sweeper." + name + " must be > 0");
        }
    }

    private static void positive(Duration d, String name) {
        if (d == null || d.isNegative() || d.isZero()) {
            throw new IllegalArgumentException("app.sweeper." + name + " must be > 0");
        }
    }
}
