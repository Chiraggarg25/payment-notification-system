package com.paynotify.common.store;

import java.util.Optional;

/**
 * Result of {@link NotificationStore#tryLockAndLoad}: the caller now owns the transaction lock
 * (identified by {@code token}) and gets the record as it was at lock time.
 * Always release with {@link NotificationStore#unlock(String, String)} in a finally block.
 */
public record LockedRecord(String member, String token, Optional<NotificationRecord> record) {
}
