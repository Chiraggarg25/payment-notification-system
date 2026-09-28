package com.paynotify.mockvendor;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;
import java.util.List;

/**
 * @param vendors             vendors known at startup (others are created on first call)
 * @param timeoutDelay        how long TIMEOUT mode sleeps (should exceed the client's read timeout)
 * @param flakyFailureRate    probability [0..1] that FLAKY mode fails a call
 * @param maxRecent           received notifications kept per vendor for the admin endpoint
 * @param maxRememberedKeys   idempotency keys remembered per vendor for duplicate detection (bounded memory)
 */
@ConfigurationProperties(prefix = "mock")
public record MockVendorProperties(
        @DefaultValue({"udemy", "amazon", "coursera"}) List<String> vendors,
        @DefaultValue("5s") Duration timeoutDelay,
        @DefaultValue("0.5") double flakyFailureRate,
        @DefaultValue("50") int maxRecent,
        @DefaultValue("10000") int maxRememberedKeys) {

    public MockVendorProperties {
        vendors = vendors == null ? List.of() : List.copyOf(vendors);
        if (timeoutDelay == null || timeoutDelay.isNegative()) {
            throw new IllegalArgumentException("mock.timeout-delay must be >= 0");
        }
        if (flakyFailureRate < 0 || flakyFailureRate > 1) {
            throw new IllegalArgumentException("mock.flaky-failure-rate must be within [0, 1]");
        }
        if (maxRecent < 1 || maxRememberedKeys < 1) {
            throw new IllegalArgumentException("mock.max-recent and mock.max-remembered-keys must be >= 1");
        }
    }
}
