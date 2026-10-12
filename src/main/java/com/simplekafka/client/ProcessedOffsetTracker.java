package com.simplekafka.client;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks the highest processed offset per (topic, partition) so that
 * consumers can skip already-processed messages after a crash-restart.
 *
 * This implements the "at-least-once + idempotent processing" pattern:
 * the consumer commits offsets after processing, and uses this tracker
 * to detect and skip redelivered messages.
 *
 * Usage:
 * <pre>
 * ProcessedOffsetTracker tracker = new ProcessedOffsetTracker();
 * tracker.loadFrom(consumer); // load committed offsets as baseline
 *
 * for (ConsumerBatch batch : consumer.poll(1024)) {
 *     for (int i = 0; i < batch.messages().size(); i++) {
 *         long offset = batch.startOffset() + i;
 *         if (tracker.isProcessed(batch.topic(), batch.partition(), offset)) {
 *             continue; // skip — already processed before crash
 *         }
 *         process(batch.messages().get(i));
 *         tracker.markProcessed(batch.topic(), batch.partition(), offset);
 *     }
 *     tracker.commitAll(consumer);
 * }
 * </pre>
 */
public class ProcessedOffsetTracker {

    /**
     * Key: "topic:partition" → highest offset that has been processed.
     * An offset of N means offsets 0..N have been processed.
     */
    private final ConcurrentHashMap<String, Long> processedUpTo = new ConcurrentHashMap<>();

    /**
     * Load committed offsets from the broker as the baseline.
     * Any offset that was committed is assumed to be already processed.
     */
    public void loadFrom(SimpleKafkaConsumer consumer) throws IOException {
        for (var tp : consumer.assignedPartitions()) {
            long committed = consumer.committedOffset(tp.topic(), tp.partition());
            if (committed > 0) {
                // committed offset N means offsets 0..(N-1) are processed
                processedUpTo.put(key(tp.topic(), tp.partition()), committed - 1);
            }
        }
    }

    /**
     * Returns true if this offset has already been processed.
     */
    public boolean isProcessed(String topic, int partition, long offset) {
        Long highest = processedUpTo.get(key(topic, partition));
        return highest != null && offset <= highest;
    }

    /**
     * Mark an offset as successfully processed.
     */
    public void markProcessed(String topic, int partition, long offset) {
        processedUpTo.merge(key(topic, partition), offset, Math::max);
    }

    /**
     * Commit all tracked offsets to the broker.
     * Commits (highestProcessed + 1) as the next offset to read.
     */
    public void commitAll(SimpleKafkaConsumer consumer) throws IOException {
        for (Map.Entry<String, Long> entry : processedUpTo.entrySet()) {
            String[] parts = entry.getKey().split(":", 2);
            String topic = parts[0];
            int partition = Integer.parseInt(parts[1]);
            long nextOffset = entry.getValue() + 1;
            consumer.commit(topic, partition, nextOffset);
        }
    }

    private static String key(String topic, int partition) {
        return topic + ":" + partition;
    }
}
