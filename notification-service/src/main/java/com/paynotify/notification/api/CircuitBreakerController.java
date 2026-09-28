package com.paynotify.notification.api;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Comparator;
import java.util.List;

/** Simple view of the per-platform circuit breakers (also available at /actuator/circuitbreakers). */
@RestController
@RequestMapping("/api/circuit-breakers")
public class CircuitBreakerController {

    private final CircuitBreakerRegistry registry;

    public CircuitBreakerController(CircuitBreakerRegistry registry) {
        this.registry = registry;
    }

    public record CircuitBreakerView(String name, String state, float failureRate, int bufferedCalls,
                                     int failedCalls, long notPermittedCalls) {
    }

    @GetMapping
    public List<CircuitBreakerView> all() {
        return registry.getAllCircuitBreakers().stream()
                .sorted(Comparator.comparing(CircuitBreaker::getName))
                .map(cb -> {
                    CircuitBreaker.Metrics m = cb.getMetrics();
                    return new CircuitBreakerView(cb.getName(), cb.getState().name(), m.getFailureRate(),
                            m.getNumberOfBufferedCalls(), m.getNumberOfFailedCalls(), m.getNumberOfNotPermittedCalls());
                })
                .toList();
    }

    @PostMapping("/{name}/reset")
    public ResponseEntity<Void> reset(@PathVariable String name) {
        registry.find(name).ifPresent(CircuitBreaker::reset);
        return ResponseEntity.noContent().build();
    }
}
