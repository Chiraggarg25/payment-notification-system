package com.paynotify.sweeper.api;

import com.paynotify.common.store.DeliveryStatus;
import com.paynotify.common.store.NotificationRecord;
import com.paynotify.common.store.NotificationStore;
import com.paynotify.common.store.RequeueResult;
import com.paynotify.sweeper.config.SweeperProperties;
import com.paynotify.sweeper.job.FailedNotificationSweeper;
import com.paynotify.sweeper.job.SweepResult;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/** Read / operate on the notification state stored in Redis. */
@RestController
@RequestMapping("/api/notifications")
public class NotificationAdminController {

    private final NotificationStore store;
    private final FailedNotificationSweeper sweeper;
    private final SweeperProperties props;

    public NotificationAdminController(NotificationStore store, FailedNotificationSweeper sweeper, SweeperProperties props) {
        this.store = store;
        this.sweeper = sweeper;
        this.props = props;
    }

    /** Newest first; ?limit is clamped to [1, app.sweeper.max-list-limit]. */
    @GetMapping
    public List<NotificationRecord> list(@RequestParam(required = false) DeliveryStatus status,
                                         @RequestParam(defaultValue = "200") int limit) {
        int bounded = Math.max(1, Math.min(limit, props.maxListLimit()));
        return store.findAll(status, bounded);
    }

    @GetMapping("/stats")
    public Map<String, Object> stats() {
        return store.stats();
    }

    @GetMapping("/{platformId}/{transactionId}")
    public ResponseEntity<NotificationRecord> get(@PathVariable String platformId, @PathVariable String transactionId) {
        return ResponseEntity.of(store.find(platformId, transactionId));
    }

    /** Run a sweep right now instead of waiting for the scheduler (safe to run concurrently: claims + locks). */
    @PostMapping("/sweep")
    public SweepResult sweepNow() {
        return sweeper.sweepOnce();
    }

    /** Give a DEAD notification a fresh set of sweeper attempts. Only DEAD ones: DELIVERED is never re-sent. */
    @PostMapping("/{platformId}/{transactionId}/requeue")
    public ResponseEntity<Map<String, String>> requeue(@PathVariable String platformId, @PathVariable String transactionId) {
        RequeueResult result = store.requeue(NotificationStore.member(platformId, transactionId), props.lockTtl());
        HttpStatus status = switch (result) {
            case REQUEUED -> HttpStatus.ACCEPTED;
            case NOT_FOUND -> HttpStatus.NOT_FOUND;
            case NOT_DEAD, BUSY -> HttpStatus.CONFLICT;
        };
        return ResponseEntity.status(status).body(Map.of("result", result.name()));
    }

    /** Wipe all notification state (local testing only). */
    @DeleteMapping
    public Map<String, Long> deleteAll() {
        return Map.of("deletedKeys", store.deleteAll());
    }
}
