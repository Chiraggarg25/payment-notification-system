package com.paynotify.common.store;

import com.paynotify.common.event.PaymentNotificationEvent;
import com.paynotify.common.store.NotificationRecord.F;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * Redis data model
 * <pre>
 *   notification:{platformId}:{txnId}           HASH    latest state of the notification for that transaction
 *   notifications:retry-queue                   ZSET    member = {platformId}:{txnId}, score = nextRetryAt (epoch ms)
 *   notifications:dead                          SET     members whose retries are maxed out
 *   notification-events:processed:{eventId}     STRING  eventIds already handled by the consumer (idempotency, TTL)
 *   notification-locks:{platformId}:{txnId}     STRING  per-transaction lock (token, TTL) held while sending
 * </pre>
 * <b>Concurrency contract:</b> every state change of a notification (consumer, sweeper, requeue) is made while
 * holding that transaction's lock, and hash + indexes are updated together in one MULTI/EXEC. So the consumer
 * and the sweeper can never send / overwrite the same transaction concurrently.
 * <p>
 * <b>No N+1:</b> multi-record reads are pipelined in chunks (one round trip per chunk, not per key).
 * Single-node Redis is assumed (multi-key Lua scripts); for Redis Cluster add hash tags to the keys.
 */
public class NotificationStore {

    private static final Logger log = LoggerFactory.getLogger(NotificationStore.class);

    public static final String KEY_PREFIX = "notification:";
    public static final String RETRY_QUEUE = "notifications:retry-queue";
    public static final String DEAD_SET = "notifications:dead";
    public static final String PROCESSED_PREFIX = "notification-events:processed:";
    public static final String LOCK_PREFIX = "notification-locks:";

    /**
     * Atomically picks up to N due members and pushes their score to "now + lease" so that other sweeper
     * instances do not pick them concurrently. If a sweeper crashes mid-way the item simply becomes due
     * again after the lease expires (no item is ever lost).
     */
    @SuppressWarnings("rawtypes")
    private static final RedisScript<List> CLAIM_DUE_SCRIPT = new DefaultRedisScript<>("""
            local items = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]))
            for _, member in ipairs(items) do
              redis.call('ZADD', KEYS[1], ARGV[3], member)
            end
            return items
            """, List.class);

    /** Acquire the transaction lock AND read the hash in ONE round trip. Returns {'BUSY'} or {'LOCKED', f1, v1, ...}. */
    @SuppressWarnings("rawtypes")
    private static final RedisScript<List> LOCK_AND_LOAD_SCRIPT = new DefaultRedisScript<>("""
            if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', tonumber(ARGV[2])) then
              local h = redis.call('HGETALL', KEYS[2])
              table.insert(h, 1, 'LOCKED')
              return h
            end
            return {'BUSY'}
            """, List.class);

    /** Release only if we still own the lock (compare token), so an expired lock re-acquired by someone else is safe. */
    private static final RedisScript<Long> UNLOCK_SCRIPT = new DefaultRedisScript<>("""
            if redis.call('GET', KEYS[1]) == ARGV[1] then
              return redis.call('DEL', KEYS[1])
            end
            return 0
            """, Long.class);

    private static final RedisScript<Long> RESCHEDULE_SCRIPT = new DefaultRedisScript<>(
            "return redis.call('ZADD', KEYS[1], 'XX', ARGV[1], ARGV[2])", Long.class);

    private final StringRedisTemplate redis;
    private final RedisStoreProperties props;

    public NotificationStore(StringRedisTemplate redis, RedisStoreProperties props) {
        this.redis = Objects.requireNonNull(redis, "redis");
        this.props = Objects.requireNonNull(props, "props");
    }

    public static String member(String platformId, String transactionId) {
        Objects.requireNonNull(platformId, "platformId");
        Objects.requireNonNull(transactionId, "transactionId");
        return platformId + ":" + transactionId;
    }

    public static String key(String member) {
        return KEY_PREFIX + member;
    }

    private static String lockKey(String member) {
        return LOCK_PREFIX + member;
    }

    private static String processedKey(String eventId) {
        return PROCESSED_PREFIX + eventId;
    }

    // ================================================================== locking

    /** Non-blocking: lock + load, or empty if another worker holds the lock. */
    public Optional<LockedRecord> tryLockAndLoad(String member, Duration ttl) {
        String token = UUID.randomUUID().toString();
        @SuppressWarnings("unchecked")
        List<Object> result = redis.execute(LOCK_AND_LOAD_SCRIPT, List.of(lockKey(member), key(member)),
                token, String.valueOf(ttl.toMillis()));
        if (result == null || result.isEmpty() || !"LOCKED".equals(String.valueOf(result.get(0)))) {
            return Optional.empty();
        }
        Map<String, String> hash = new HashMap<>();
        for (int i = 1; i + 1 < result.size(); i += 2) {
            Object k = result.get(i);
            Object v = result.get(i + 1);
            if (k != null && v != null) {
                hash.put(String.valueOf(k), String.valueOf(v));
            }
        }
        Optional<NotificationRecord> record = hash.isEmpty() ? Optional.empty() : Optional.of(NotificationRecord.fromHash(hash));
        return Optional.of(new LockedRecord(member, token, record));
    }

    /**
     * Bounded wait for the lock: polls every {@code pollInterval} until {@code maxWait} has elapsed.
     * Only used from the Kafka consumer thread (a dedicated blocking thread, not an async/event-loop thread).
     */
    public Optional<LockedRecord> lockAndLoad(String member, Duration ttl, Duration maxWait, Duration pollInterval) {
        long deadline = System.nanoTime() + maxWait.toNanos();
        while (true) {
            Optional<LockedRecord> locked = tryLockAndLoad(member, ttl);
            if (locked.isPresent() || System.nanoTime() >= deadline) {
                return locked;
            }
            try {
                Thread.sleep(pollInterval.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return Optional.empty();
            }
        }
    }

    public void unlock(String member, String token) {
        if (member == null || token == null) {
            return;
        }
        try {
            redis.execute(UNLOCK_SCRIPT, List.of(lockKey(member)), token);
        } catch (DataAccessException e) {
            // Lock has a TTL, it will expire on its own - never fail the business operation because of this.
            log.warn("[REDIS] could not release lock for {}: {}", member, e.getMessage());
        }
    }

    // ================================================================== reads

    public Optional<NotificationRecord> find(String platformId, String transactionId) {
        return findByMember(member(platformId, transactionId));
    }

    public Optional<NotificationRecord> findByMember(String member) {
        Map<Object, Object> raw = redis.opsForHash().entries(key(member));
        return toRecord(raw);
    }

    public boolean isProcessed(String eventId) {
        return eventId != null && Boolean.TRUE.equals(redis.hasKey(processedKey(eventId)));
    }

    /**
     * Admin/list query. SCAN (non-blocking for Redis, unlike KEYS) + pipelined HGETALL per chunk:
     * one round trip per {@code scanBatchSize} keys instead of one per key.
     */
    public List<NotificationRecord> findAll(DeliveryStatus statusFilter, int limit) {
        List<NotificationRecord> result = new ArrayList<>();
        for (List<String> chunk : chunks(scanKeys(KEY_PREFIX + "*"))) {
            List<Object> hashes = pipeline(ops -> chunk.forEach(k -> ops.opsForHash().entries(k)));
            for (Object o : hashes) {
                if (o instanceof Map<?, ?> map) {
                    toRecord(map)
                            .filter(r -> statusFilter == null || r.deliveryStatus() == statusFilter)
                            .ifPresent(result::add);
                }
            }
        }
        result.sort(Comparator.comparing(
                (NotificationRecord r) -> r.updatedAt() == null ? "" : r.updatedAt()).reversed());
        return result.size() > limit ? List.copyOf(result.subList(0, limit)) : result;
    }

    /** Counts per status with pipelined HGET (only the status field is transferred). */
    public Map<String, Object> stats() {
        Map<DeliveryStatus, Long> byStatus = new EnumMap<>(DeliveryStatus.class);
        for (DeliveryStatus s : DeliveryStatus.values()) {
            byStatus.put(s, 0L);
        }
        for (List<String> chunk : chunks(scanKeys(KEY_PREFIX + "*"))) {
            List<Object> statuses = pipeline(ops -> chunk.forEach(k -> ops.opsForHash().get(k, F.DELIVERY_STATUS)));
            for (Object s : statuses) {
                if (s != null) {
                    try {
                        byStatus.merge(DeliveryStatus.valueOf(String.valueOf(s)), 1L, Long::sum);
                    } catch (IllegalArgumentException ignored) {
                        // corrupt value - not counted
                    }
                }
            }
        }
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("byStatus", byStatus);
        Long queued = redis.opsForZSet().zCard(RETRY_QUEUE);
        Long dead = redis.opsForSet().size(DEAD_SET);
        stats.put("retryQueueSize", queued == null ? 0 : queued);
        stats.put("deadSetSize", dead == null ? 0 : dead);
        return stats;
    }

    // ================================================================== writes (consumer) - caller holds the lock

    public void markDelivered(PaymentNotificationEvent event, int consumerAttempts, int sweepAttempts, DeliveredBy by) {
        String member = member(event.platformId(), event.transactionId());
        String now = Instant.now().toString();
        Map<String, String> fields = eventFields(event);
        fields.put(F.DELIVERY_STATUS, DeliveryStatus.DELIVERED.name());
        fields.put(F.CONSUMER_ATTEMPTS, String.valueOf(consumerAttempts));
        fields.put(F.SWEEP_ATTEMPTS, String.valueOf(sweepAttempts));
        fields.put(F.LAST_ATTEMPT_AT, now);
        fields.put(F.DELIVERED_AT, now);
        fields.put(F.DELIVERED_BY, by.name());
        fields.put(F.UPDATED_AT, now);
        transaction(ops -> {
            ops.opsForHash().putAll(key(member), fields);
            ops.opsForHash().delete(key(member), F.NEXT_RETRY_AT);
            ops.expire(key(member), props.deliveredTtl());
            ops.opsForZSet().remove(RETRY_QUEUE, member);
            ops.opsForSet().remove(DEAD_SET, member);
            ops.opsForValue().set(processedKey(event.eventId()), by.name(), props.processedEventTtl());
        });
        log.info("[REDIS] {} -> DELIVERED by {} (consumerAttempts={}, sweepAttempts={})",
                member, by, consumerAttempts, sweepAttempts);
    }

    /** Called by the consumer after retries / circuit breaker are exhausted. */
    public void markRetryPending(PaymentNotificationEvent event, int consumerAttempts, String error, Instant nextRetryAt) {
        String member = member(event.platformId(), event.transactionId());
        String now = Instant.now().toString();
        Map<String, String> fields = eventFields(event);
        fields.put(F.DELIVERY_STATUS, DeliveryStatus.RETRY_PENDING.name());
        fields.put(F.CONSUMER_ATTEMPTS, String.valueOf(consumerAttempts));
        fields.put(F.SWEEP_ATTEMPTS, "0");
        fields.put(F.POSTPONES, "0");
        fields.put(F.LAST_ERROR, nullToEmpty(error));
        fields.put(F.FIRST_FAILED_AT, now);
        fields.put(F.LAST_ATTEMPT_AT, now);
        fields.put(F.NEXT_RETRY_AT, nextRetryAt.toString());
        fields.put(F.UPDATED_AT, now);
        transaction(ops -> {
            ops.opsForHash().putAll(key(member), fields);
            ops.opsForHash().delete(key(member), F.DELIVERED_AT, F.DELIVERED_BY);
            ops.persist(key(member));
            ops.opsForZSet().add(RETRY_QUEUE, member, nextRetryAt.toEpochMilli());
            ops.opsForSet().remove(DEAD_SET, member);
            ops.opsForValue().set(processedKey(event.eventId()), "PARKED", props.processedEventTtl());
        });
        log.warn("[REDIS] {} -> RETRY_PENDING (consumerAttempts={}, nextRetryAt={}, error={})",
                member, consumerAttempts, nextRetryAt, error);
    }

    /** Permanent failure detected by the consumer (e.g. HTTP 400). */
    public void markDead(PaymentNotificationEvent event, int consumerAttempts, String reason) {
        String member = member(event.platformId(), event.transactionId());
        Map<String, String> fields = eventFields(event);
        fields.put(F.CONSUMER_ATTEMPTS, String.valueOf(consumerAttempts));
        fields.put(F.SWEEP_ATTEMPTS, "0");
        fields.put(F.FIRST_FAILED_AT, Instant.now().toString());
        writeDead(member, fields, reason, event.eventId());
    }

    /** Remember an eventId without touching the notification (e.g. a stale, superseded event). */
    public void markProcessed(String eventId, String marker) {
        if (eventId != null) {
            redis.opsForValue().set(processedKey(eventId), marker, props.processedEventTtl());
        }
    }

    // ================================================================== writes (sweeper) - caller holds the lock

    public List<String> claimDue(Instant now, int batchSize, Duration lease) {
        @SuppressWarnings("unchecked")
        List<Object> claimed = redis.execute(CLAIM_DUE_SCRIPT, List.of(RETRY_QUEUE),
                String.valueOf(now.toEpochMilli()),
                String.valueOf(batchSize),
                String.valueOf(now.plus(lease).toEpochMilli()));
        if (claimed == null) {
            return List.of();
        }
        return claimed.stream().filter(Objects::nonNull).map(String::valueOf).toList();
    }

    public void recordSweepFailure(String member, int sweepAttempts, String error, Instant nextRetryAt) {
        String now = Instant.now().toString();
        Map<String, String> fields = new HashMap<>();
        fields.put(F.SWEEP_ATTEMPTS, String.valueOf(sweepAttempts));
        fields.put(F.LAST_ERROR, nullToEmpty(error));
        fields.put(F.LAST_ATTEMPT_AT, now);
        fields.put(F.NEXT_RETRY_AT, nextRetryAt.toString());
        fields.put(F.UPDATED_AT, now);
        transaction(ops -> {
            ops.opsForHash().putAll(key(member), fields);
            ops.opsForZSet().add(RETRY_QUEUE, member, nextRetryAt.toEpochMilli());
        });
        log.warn("[REDIS] {} sweep attempt #{} failed -> next retry at {} ({})", member, sweepAttempts, nextRetryAt, error);
    }

    /** Circuit open in the sweeper: push back without consuming a send attempt (bounded by max-postpones). */
    public void postpone(String member, int postpones, String reason, Instant nextRetryAt) {
        Map<String, String> fields = new HashMap<>();
        fields.put(F.POSTPONES, String.valueOf(postpones));
        fields.put(F.LAST_ERROR, nullToEmpty(reason));
        fields.put(F.NEXT_RETRY_AT, nextRetryAt.toString());
        fields.put(F.UPDATED_AT, Instant.now().toString());
        transaction(ops -> {
            ops.opsForHash().putAll(key(member), fields);
            ops.opsForZSet().add(RETRY_QUEUE, member, nextRetryAt.toEpochMilli());
        });
        log.info("[REDIS] {} postponed (#{}) to {} ({})", member, postpones, nextRetryAt, reason);
    }

    /**
     * Lock busy: move the claimed item to a new time. ZADD XX only updates members that are still queued,
     * so an item that was delivered/removed meanwhile is never resurrected.
     */
    public void reschedule(String member, Instant at) {
        redis.execute(RESCHEDULE_SCRIPT, List.of(RETRY_QUEUE), String.valueOf(at.toEpochMilli()), member);
    }

    public void markDead(String member, int sweepAttempts, String reason) {
        Map<String, String> fields = new HashMap<>();
        fields.put(F.SWEEP_ATTEMPTS, String.valueOf(sweepAttempts));
        writeDead(member, fields, reason, null);
    }

    public void removeFromQueue(String member) {
        redis.opsForZSet().remove(RETRY_QUEUE, member);
    }

    /** Manually put a DEAD notification back in the retry queue with a fresh set of attempts. */
    public RequeueResult requeue(String member, Duration lockTtl) {
        Optional<LockedRecord> locked = tryLockAndLoad(member, lockTtl);
        if (locked.isEmpty()) {
            return RequeueResult.BUSY;
        }
        try {
            Optional<NotificationRecord> record = locked.get().record();
            if (record.isEmpty()) {
                return RequeueResult.NOT_FOUND;
            }
            if (record.get().deliveryStatus() != DeliveryStatus.DEAD) {
                return RequeueResult.NOT_DEAD;
            }
            Instant now = Instant.now();
            Map<String, String> fields = new HashMap<>();
            fields.put(F.DELIVERY_STATUS, DeliveryStatus.RETRY_PENDING.name());
            fields.put(F.SWEEP_ATTEMPTS, "0");
            fields.put(F.POSTPONES, "0");
            fields.put(F.NEXT_RETRY_AT, now.toString());
            fields.put(F.UPDATED_AT, now.toString());
            transaction(ops -> {
                ops.opsForHash().putAll(key(member), fields);
                ops.persist(key(member));
                ops.opsForZSet().add(RETRY_QUEUE, member, now.toEpochMilli());
                ops.opsForSet().remove(DEAD_SET, member);
            });
            log.info("[REDIS] {} manually re-queued", member);
            return RequeueResult.REQUEUED;
        } finally {
            unlock(member, locked.get().token());
        }
    }

    /** Deletes every key of this store (local testing only). UNLINK in chunks: non-blocking for Redis. */
    public long deleteAll() {
        List<String> keys = new ArrayList<>();
        keys.addAll(scanKeys(KEY_PREFIX + "*"));
        keys.addAll(scanKeys(PROCESSED_PREFIX + "*"));
        keys.addAll(scanKeys(LOCK_PREFIX + "*"));
        keys.add(RETRY_QUEUE);
        keys.add(DEAD_SET);
        long deleted = 0;
        for (List<String> chunk : chunks(keys)) {
            Long n = redis.unlink(chunk);
            deleted += n == null ? 0 : n;
        }
        return deleted;
    }

    // ================================================================== helpers

    private void writeDead(String member, Map<String, String> fields, String reason, String eventId) {
        String now = Instant.now().toString();
        fields.put(F.DELIVERY_STATUS, DeliveryStatus.DEAD.name());
        fields.put(F.LAST_ERROR, nullToEmpty(reason));
        fields.put(F.LAST_ATTEMPT_AT, now);
        fields.put(F.UPDATED_AT, now);
        transaction(ops -> {
            ops.opsForHash().putAll(key(member), fields);
            ops.opsForHash().delete(key(member), F.NEXT_RETRY_AT);
            ops.persist(key(member));
            ops.opsForZSet().remove(RETRY_QUEUE, member);
            ops.opsForSet().add(DEAD_SET, member);
            if (eventId != null) {
                ops.opsForValue().set(processedKey(eventId), "DEAD", props.processedEventTtl());
            }
        });
        log.error("[REDIS] {} -> DEAD ({})", member, reason);
    }

    private static Optional<NotificationRecord> toRecord(Map<?, ?> raw) {
        if (raw == null || raw.isEmpty()) {
            return Optional.empty();
        }
        Map<String, String> h = new HashMap<>();
        raw.forEach((k, v) -> {
            if (k != null && v != null) {
                h.put(String.valueOf(k), String.valueOf(v));
            }
        });
        return Optional.of(NotificationRecord.fromHash(h));
    }

    private static Map<String, String> eventFields(PaymentNotificationEvent event) {
        Map<String, String> fields = new HashMap<>();
        fields.put(F.EVENT_ID, event.eventId());
        fields.put(F.PLATFORM_ID, event.platformId());
        fields.put(F.TRANSACTION_ID, event.transactionId());
        fields.put(F.PAYMENT_STATUS, event.paymentStatus().name());
        fields.put(F.CREATED_AT, event.createdAt() == null ? Instant.now().toString() : event.createdAt().toString());
        return fields;
    }

    private static String nullToEmpty(String s) {
        return s == null ? "" : s;
    }

    private List<String> scanKeys(String pattern) {
        List<String> keys = new ArrayList<>();
        ScanOptions options = ScanOptions.scanOptions().match(pattern).count(props.scanBatchSize()).build();
        try (Cursor<String> cursor = redis.scan(options)) {
            cursor.forEachRemaining(keys::add);
        }
        return keys;
    }

    private List<List<String>> chunks(List<String> keys) {
        List<List<String>> chunks = new ArrayList<>();
        int size = props.scanBatchSize();
        for (int i = 0; i < keys.size(); i += size) {
            chunks.add(keys.subList(i, Math.min(i + size, keys.size())));
        }
        return chunks;
    }

    private List<Object> pipeline(Consumer<RedisOperations<String, String>> body) {
        List<Object> results = redis.executePipelined(new SessionCallback<Object>() {
            @Override
            @SuppressWarnings("unchecked")
            public <K, V> Object execute(RedisOperations<K, V> operations) throws DataAccessException {
                body.accept((RedisOperations<String, String>) operations);
                return null;
            }
        });
        return results == null ? List.of() : results;
    }

    private void transaction(Consumer<RedisOperations<String, String>> body) {
        redis.execute(new SessionCallback<List<Object>>() {
            @Override
            @SuppressWarnings("unchecked")
            public <K, V> List<Object> execute(RedisOperations<K, V> operations) throws DataAccessException {
                RedisOperations<String, String> ops = (RedisOperations<String, String>) operations;
                ops.multi();
                body.accept(ops);
                return ops.exec();
            }
        });
    }
}
