package com.paynotify.mockvendor;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/** Thread-safe state of one fake platform. All memory is bounded. */
public class VendorState {

    public record ReceivedNotification(Instant receivedAt, String idempotencyKey, boolean duplicate, Map<String, Object> body) {
    }

    private final String name;
    private final int maxRecent;
    private volatile VendorMode mode = VendorMode.UP;
    private final AtomicLong totalCalls = new AtomicLong();
    private final AtomicLong acknowledged = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();

    private final Object lock = new Object();
    private final Deque<ReceivedNotification> recent = new ArrayDeque<>();   // guarded by lock
    private final Map<String, Boolean> seenKeys;                              // guarded by lock (LRU)

    public VendorState(String name, int maxRecent, int maxRememberedKeys) {
        this.name = name;
        this.maxRecent = maxRecent;
        this.seenKeys = new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                return size() > maxRememberedKeys;
            }
        };
    }

    public String name() {
        return name;
    }

    public VendorMode mode() {
        return mode;
    }

    public void mode(VendorMode mode) {
        this.mode = mode;
    }

    public void countCall() {
        totalCalls.incrementAndGet();
    }

    public void countFailure() {
        failed.incrementAndGet();
    }

    /** @return true if this idempotency key was seen before */
    public boolean accept(String idempotencyKey, Map<String, Object> body) {
        synchronized (lock) {
            boolean duplicate = idempotencyKey != null && seenKeys.put(idempotencyKey, Boolean.TRUE) != null;
            recent.addFirst(new ReceivedNotification(Instant.now(), idempotencyKey, duplicate, body));
            while (recent.size() > maxRecent) {
                recent.pollLast();
            }
            acknowledged.incrementAndGet();
            return duplicate;
        }
    }

    public List<ReceivedNotification> recent() {
        synchronized (lock) {
            return new ArrayList<>(recent);
        }
    }

    public void reset() {
        synchronized (lock) {
            mode = VendorMode.UP;
            totalCalls.set(0);
            acknowledged.set(0);
            failed.set(0);
            recent.clear();
            seenKeys.clear();
        }
    }

    public Map<String, Object> view() {
        return Map.of(
                "name", name,
                "mode", mode.name(),
                "totalCalls", totalCalls.get(),
                "acknowledged", acknowledged.get(),
                "failed", failed.get());
    }
}
