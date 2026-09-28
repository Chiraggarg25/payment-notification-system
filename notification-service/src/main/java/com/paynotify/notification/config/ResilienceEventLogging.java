package com.paynotify.notification.config;

import com.paynotify.common.client.PlatformProperties;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryRegistry;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Logs retry attempts (with the exponential wait) and circuit breaker state transitions, and pre-creates a
 * Retry + CircuitBreaker for every platform in platforms.yml (no platform names hardcoded in YAML/code).
 */
@Component
public class ResilienceEventLogging {

    private static final Logger log = LoggerFactory.getLogger(ResilienceEventLogging.class);

    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final RetryRegistry retryRegistry;
    private final PlatformProperties platforms;

    public ResilienceEventLogging(CircuitBreakerRegistry circuitBreakerRegistry,
                                  RetryRegistry retryRegistry,
                                  PlatformProperties platforms) {
        this.circuitBreakerRegistry = circuitBreakerRegistry;
        this.retryRegistry = retryRegistry;
        this.platforms = platforms;
    }

    @PostConstruct
    void register() {
        circuitBreakerRegistry.getAllCircuitBreakers().forEach(this::attach);
        circuitBreakerRegistry.getEventPublisher().onEntryAdded(e -> attach(e.getAddedEntry()));
        retryRegistry.getAllRetries().forEach(this::attach);
        retryRegistry.getEventPublisher().onEntryAdded(e -> attach(e.getAddedEntry()));
        // create them up front so they are visible (actuator / UI) before the first event
        platforms.platformIds().forEach(id -> {
            circuitBreakerRegistry.circuitBreaker(id);
            retryRegistry.retry(id);
        });
    }

    private void attach(CircuitBreaker cb) {
        cb.getEventPublisher()
                .onStateTransition(e -> log.warn("[CIRCUIT-BREAKER] '{}' {}", e.getCircuitBreakerName(),
                        e.getStateTransition()))
                .onCallNotPermitted(e -> log.warn("[CIRCUIT-BREAKER] '{}' is OPEN -> call short-circuited",
                        e.getCircuitBreakerName()));
    }

    private void attach(Retry retry) {
        retry.getEventPublisher()
                .onRetry(e -> log.warn("[RETRY] '{}' attempt #{} failed ({}); waiting {} ms before next attempt",
                        e.getName(), e.getNumberOfRetryAttempts(),
                        e.getLastThrowable() == null ? "?" : e.getLastThrowable().getMessage(),
                        e.getWaitInterval().toMillis()))
                .onError(e -> log.error("[RETRY] '{}' exhausted after {} attempts: {}",
                        e.getName(), e.getNumberOfRetryAttempts(),
                        e.getLastThrowable() == null ? "?" : e.getLastThrowable().getMessage()));
    }
}
