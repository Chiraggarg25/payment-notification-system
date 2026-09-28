package com.paynotify.notification.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/** All notification-service tunables (application.yml -> app.*). Validated at startup (fail fast). */
@ConfigurationProperties(prefix = "app")
public record NotificationProperties(@DefaultValue Kafka kafka, @DefaultValue Delivery delivery) {

    /**
     * @param groupIdPrefix  consumer group per platform = prefix + platformId
     * @param clientIdPrefix Kafka client.id per platform = prefix + platformId
     * @param errorHandler   bounded re-processing when the listener throws (e.g. Redis down)
     */
    public record Kafka(@DefaultValue("notification-service.") String groupIdPrefix,
                        @DefaultValue("notification-") String clientIdPrefix,
                        @DefaultValue ErrorHandler errorHandler) {
    }

    /**
     * Exponential backoff of the Kafka error handler. After {@code maxElapsedTime} the record is published to
     * the platform's dead-letter topic and its offset committed -> retries are bounded, nothing is lost.
     */
    public record ErrorHandler(@DefaultValue("1s") Duration initialInterval,
                               @DefaultValue("2.0") double multiplier,
                               @DefaultValue("30s") Duration maxInterval,
                               @DefaultValue("5m") Duration maxElapsedTime) {
        public ErrorHandler {
            positive(initialInterval, "app.kafka.error-handler.initial-interval");
            positive(maxInterval, "app.kafka.error-handler.max-interval");
            positive(maxElapsedTime, "app.kafka.error-handler.max-elapsed-time");
            if (multiplier < 1.0) {
                throw new IllegalArgumentException("app.kafka.error-handler.multiplier must be >= 1.0");
            }
        }
    }

    /**
     * @param firstRetryDelay  when the sweeper should first look at a parked event
     * @param lockTtl          per-transaction lock lease while the consumer delivers; must exceed the worst-case
     *                         delivery time (retries x read-timeout + backoff, ~20s with the defaults)
     * @param lockWait         max time the consumer waits for a lock held by the sweeper (bounded)
     * @param lockPollInterval poll interval while waiting for the lock
     */
    public record Delivery(@DefaultValue("10s") Duration firstRetryDelay,
                           @DefaultValue("60s") Duration lockTtl,
                           @DefaultValue("10s") Duration lockWait,
                           @DefaultValue("100ms") Duration lockPollInterval) {
        public Delivery {
            positive(firstRetryDelay, "app.delivery.first-retry-delay");
            positive(lockTtl, "app.delivery.lock-ttl");
            positive(lockWait, "app.delivery.lock-wait");
            positive(lockPollInterval, "app.delivery.lock-poll-interval");
        }
    }

    private static void positive(Duration d, String name) {
        if (d == null || d.isNegative() || d.isZero()) {
            throw new IllegalArgumentException(name + " must be > 0");
        }
    }
}
