package com.paynotify.sweeper.job;

/**
 * @param busy     items whose transaction lock was held by the consumer (re-scheduled shortly)
 * @param errors   items that failed unexpectedly (e.g. Redis error) - they stay claimed and come back after the lease
 * @param timedOut platform batches that did not finish within sweep-timeout (their items come back after the lease)
 */
public record SweepResult(int claimed, int delivered, int rescheduled, int postponed, int dead, int skipped,
                          int busy, int errors, int timedOut) {

    public static SweepResult empty() {
        return new SweepResult(0, 0, 0, 0, 0, 0, 0, 0, 0);
    }
}
