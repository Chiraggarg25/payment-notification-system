package com.paynotify.platform.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/**
 * @param publishTimeout       max time to wait for the Kafka ack before answering 503
 * @param maxIdempotencyKeyLength longest Idempotency-Key header accepted
 */
@ConfigurationProperties(prefix = "app")
public record PlatformServiceProperties(
        @DefaultValue("10s") Duration publishTimeout,
        @DefaultValue("128") int maxIdempotencyKeyLength) {

    public PlatformServiceProperties {
        if (publishTimeout == null || publishTimeout.isNegative() || publishTimeout.isZero()) {
            throw new IllegalArgumentException("app.publish-timeout must be > 0");
        }
        if (maxIdempotencyKeyLength < 1) {
            throw new IllegalArgumentException("app.max-idempotency-key-length must be >= 1");
        }
    }
}
