package com.paynotify.common.store;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;
import java.util.Objects;

/**
 * @param deliveredTtl      how long DELIVERED hashes are kept (failures are kept until handled)
 * @param processedEventTtl how long processed eventIds are remembered for de-duplication.
 *                          Keep it >= the Kafka topic retention so a replay can never re-send an old event.
 * @param scanBatchSize     SCAN COUNT hint and pipeline chunk size for admin/list queries
 */
@ConfigurationProperties(prefix = "app.redis")
public record RedisStoreProperties(
        @DefaultValue("7d") Duration deliveredTtl,
        @DefaultValue("7d") Duration processedEventTtl,
        @DefaultValue("500") int scanBatchSize) {

    public RedisStoreProperties {
        requirePositive(deliveredTtl, "app.redis.delivered-ttl");
        requirePositive(processedEventTtl, "app.redis.processed-event-ttl");
        if (scanBatchSize <= 0) {
            throw new IllegalArgumentException("app.redis.scan-batch-size must be > 0");
        }
    }

    static void requirePositive(Duration d, String name) {
        Objects.requireNonNull(d, name + " must be set");
        if (d.isNegative() || d.isZero()) {
            throw new IllegalArgumentException(name + " must be > 0");
        }
    }
}
