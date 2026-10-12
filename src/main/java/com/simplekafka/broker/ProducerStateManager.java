package com.simplekafka.broker;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages producer identities and tracks the last accepted sequence number
 * per (producerId, topic, partition) to enable idempotent produce requests.
 *
 * Dedup logic:
 *   seq == lastSeq     → DUPLICATE (return cached offset, don't write)
 *   seq == lastSeq + 1 → NEW message (update lastSeq, allow write)
 *   seq > lastSeq + 1  → GAP (throw OutOfOrderSequenceException)
 *   seq < lastSeq      → OLD DUPLICATE (return -1, skip)
 *
 * First message for a new (pid, topic, partition) must have seq == 0.
 */
class ProducerStateManager {

    private final AtomicLong nextPid = new AtomicLong(1);

    /**
     * Key: "pid:topic:partition" → ProducerPartitionState
     */
    private final ConcurrentHashMap<String, ProducerPartitionState> sequences =
        new ConcurrentHashMap<>();

    /**
     * Allocates a unique producer ID.
     */
    long allocateProducerId() {
        return nextPid.getAndIncrement();
    }

    /**
     * Checks the sequence number and returns a dedup result.
     * Must be called BEFORE writing to the partition log.
     *
     * @return a DeduplicationResult indicating whether to write or skip
     * @throws OutOfOrderSequenceException if there's a sequence gap
     */
    DeduplicationResult checkSequence(long producerId, String topic, int partition, int sequenceNum) {
        String key = producerId + ":" + topic + ":" + partition;

        ProducerPartitionState state = sequences.get(key);

        if (state == null) {
            // First message from this producer to this partition
            if (sequenceNum != 0) {
                throw new OutOfOrderSequenceException(
                    "First message for PID=" + producerId + " topic=" + topic +
                    " partition=" + partition + " must have seq=0, got seq=" + sequenceNum
                );
            }
            // Will be recorded after successful write via recordOffset()
            return new DeduplicationResult.NewMessage();
        }

        synchronized (state) {
            if (sequenceNum == state.lastSequence) {
                // Exact retry — return the cached offset
                return new DeduplicationResult.Duplicate(state.lastOffset);
            }

            if (sequenceNum < state.lastSequence) {
                // Old retry (already processed and moved past)
                return new DeduplicationResult.Duplicate(state.lastOffset);
            }

            if (sequenceNum == state.lastSequence + 1) {
                // Next expected sequence — new message
                return new DeduplicationResult.NewMessage();
            }

            // seq > lastSeq + 1 → gap detected
            throw new OutOfOrderSequenceException(
                "Expected seq=" + (state.lastSequence + 1) + " for PID=" + producerId +
                " topic=" + topic + " partition=" + partition +
                ", got seq=" + sequenceNum + " (gap of " + (sequenceNum - state.lastSequence - 1) + " messages)"
            );
        }
    }

    /**
     * Records that a message was successfully written.
     * Must be called AFTER writing to the partition log.
     */
    void recordOffset(long producerId, String topic, int partition, int sequenceNum, long offset) {
        String key = producerId + ":" + topic + ":" + partition;

        sequences.compute(key, (k, existing) -> {
            if (existing == null) {
                return new ProducerPartitionState(sequenceNum, offset);
            }
            synchronized (existing) {
                existing.lastSequence = sequenceNum;
                existing.lastOffset = offset;
            }
            return existing;
        });
    }

    private static class ProducerPartitionState {
        int lastSequence;
        long lastOffset;

        ProducerPartitionState(int lastSequence, long lastOffset) {
            this.lastSequence = lastSequence;
            this.lastOffset = lastOffset;
        }
    }

    /**
     * Result of a deduplication check.
     */
    sealed interface DeduplicationResult {
        record NewMessage() implements DeduplicationResult {}
        record Duplicate(long cachedOffset) implements DeduplicationResult {}
    }
}
