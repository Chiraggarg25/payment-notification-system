package com.paynotify.mockvendor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;

/**
 * Pretends to be the platforms' webhook endpoints:
 *   POST /{platform}/payments/notify
 * plus admin endpoints to change each platform's behaviour at runtime:
 *   GET  /admin/vendors
 *   PUT  /admin/vendors/{platform}/mode/{UP|DOWN|FLAKY|TIMEOUT|REJECT}
 *   GET  /admin/vendors/{platform}/received
 *   POST /admin/vendors/reset
 * Runs on virtual threads (spring.threads.virtual.enabled), so TIMEOUT mode's sleep is cheap.
 */
@RestController
public class MockVendorController {

    private static final Logger log = LoggerFactory.getLogger(MockVendorController.class);
    private static final Pattern VALID_NAME = Pattern.compile("[a-z0-9-]{1,40}");
    private static final int MAX_VENDORS = 100;

    private final Map<String, VendorState> vendors = new ConcurrentHashMap<>();
    private final MockVendorProperties props;

    public MockVendorController(MockVendorProperties props) {
        this.props = props;
        props.vendors().forEach(this::vendor);
    }

    private VendorState vendor(String rawName) {
        String name = rawName == null ? "" : rawName.trim().toLowerCase(Locale.ROOT);
        if (!VALID_NAME.matcher(name).matches()) {
            throw new IllegalArgumentException("Invalid platform name '" + rawName + "'");
        }
        VendorState existing = vendors.get(name);
        if (existing != null) {
            return existing;
        }
        if (vendors.size() >= MAX_VENDORS) {
            throw new IllegalArgumentException("Too many vendors (max " + MAX_VENDORS + ")");
        }
        return vendors.computeIfAbsent(name, n -> new VendorState(n, props.maxRecent(), props.maxRememberedKeys()));
    }

    @PostMapping("/{platform}/payments/notify")
    public ResponseEntity<Map<String, Object>> notify(@PathVariable String platform,
                                                      @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
                                                      @RequestBody Map<String, Object> body) throws InterruptedException {
        VendorState v = vendor(platform);
        v.countCall();
        Object txn = body == null ? null : body.get("transactionId");

        switch (v.mode()) {
            case DOWN -> {
                v.countFailure();
                log.warn("[{}] DOWN -> 503 for txn {}", platform, txn);
                return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                        .body(Map.of("error", platform + " is unavailable"));
            }
            case FLAKY -> {
                if (ThreadLocalRandom.current().nextDouble() < props.flakyFailureRate()) {
                    v.countFailure();
                    log.warn("[{}] FLAKY -> 500 for txn {}", platform, txn);
                    return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                            .body(Map.of("error", "random failure"));
                }
            }
            case TIMEOUT -> {
                v.countFailure();
                log.warn("[{}] TIMEOUT -> sleeping {} for txn {}", platform, props.timeoutDelay(), txn);
                Thread.sleep(props.timeoutDelay().toMillis());
                return ResponseEntity.ok(Map.of("status", "TOO_LATE"));
            }
            case REJECT -> {
                v.countFailure();
                log.warn("[{}] REJECT -> 400 for txn {}", platform, txn);
                return ResponseEntity.badRequest().body(Map.of("error", "invalid transaction"));
            }
            case UP -> {
                // fall through to success
            }
        }

        boolean duplicate = v.accept(idempotencyKey, body == null ? Map.of() : body);
        log.info("[{}] ACK txn={} status={} idempotencyKey={}{}", platform, txn,
                body == null ? null : body.get("paymentStatus"), idempotencyKey, duplicate ? " (DUPLICATE)" : "");
        return ResponseEntity.ok(Map.of("status", "ACKNOWLEDGED", "duplicate", duplicate));
    }

    @GetMapping("/admin/vendors")
    public List<Map<String, Object>> list() {
        return vendors.values().stream()
                .sorted(Comparator.comparing(VendorState::name))
                .map(VendorState::view)
                .toList();
    }

    @PutMapping("/admin/vendors/{platform}/mode/{mode}")
    public Map<String, Object> setMode(@PathVariable String platform, @PathVariable String mode) {
        VendorState v = vendor(platform);
        v.mode(parseMode(mode));
        log.info("[{}] mode set to {}", platform, v.mode());
        return v.view();
    }

    @GetMapping("/admin/vendors/{platform}/received")
    public List<VendorState.ReceivedNotification> received(@PathVariable String platform) {
        return vendor(platform).recent();
    }

    @PostMapping("/admin/vendors/reset")
    public List<Map<String, Object>> reset() {
        vendors.values().forEach(VendorState::reset);
        return list();
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ProblemDetail badRequest(IllegalArgumentException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, e.getMessage());
    }

    private static VendorMode parseMode(String mode) {
        try {
            return VendorMode.valueOf(mode == null ? "" : mode.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("mode must be one of " + Arrays.toString(VendorMode.values()));
        }
    }
}
