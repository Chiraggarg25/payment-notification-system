package com.paynotify.common.store;

public enum RequeueResult {
    REQUEUED,
    NOT_FOUND,
    /** Only DEAD notifications can be re-queued (a DELIVERED one must never be re-sent). */
    NOT_DEAD,
    /** Another worker holds the transaction lock right now; try again. */
    BUSY
}
