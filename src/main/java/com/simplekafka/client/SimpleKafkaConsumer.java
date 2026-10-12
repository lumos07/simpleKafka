package com.simplekafka.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.simplekafka.common.TopicPartition;
import com.simplekafka.protocol.Protocol;

public class SimpleKafkaConsumer implements AutoCloseable {
    private final String groupId;
    private final SimpleKafkaClient client;

    private String memberId = "";
    private int generationId = -1;
    private String subscribedTopic;
    private List<TopicPartition> assignedPartitions = List.of();

    public SimpleKafkaConsumer(String host, int port, String groupId) throws IOException {
        this.groupId = groupId;
        this.client = new SimpleKafkaClient(host, port);
    }

    /**
     * Subscribe to a topic and join the consumer group.
     * The broker assigns partitions via consistent hashing.
     */
    public void subscribe(String topic) throws IOException {
        Protocol.JoinGroupResult result = client.joinGroup(groupId, memberId, topic);
        if (result.hasError()) {
            throw new IOException(result.error);
        }
        this.memberId = result.memberId;
        this.generationId = result.generationId;
        this.subscribedTopic = topic;
        // Convert wire-format DTOs to domain-level TopicPartition
        this.assignedPartitions = result.assignments.stream()
            .map(tp -> new TopicPartition(tp.topic, tp.partition))
            .toList();
    }

    /**
     * Poll for messages from all assigned partitions.
     * Automatically detects rebalances via heartbeat and re-subscribes.
     */
    public List<ConsumerBatch> poll(int maxBytes) throws IOException {
        ensureSubscribed();

        // heartbeat to detect rebalance
        Protocol.HeartbeatResult hb = client.heartbeat(groupId, memberId, generationId);
        if (hb.hasError()) {
            if ("REBALANCE_IN_PROGRESS".equals(hb.error)) {
                // group membership changed — rejoin to get new assignments
                subscribe(subscribedTopic);
            } else {
                throw new IOException(hb.error);
            }
        }

        List<ConsumerBatch> batches = new ArrayList<>();
        for (TopicPartition tp : assignedPartitions) {
            long offset = committedOffset(tp.topic(), tp.partition());
            if (offset < 0) {
                offset = 0;
            }

            Protocol.FetchResult result = client.fetch(tp.topic(), tp.partition(), offset, maxBytes);
            if (result.hasError()) {
                throw new IOException(result.error);
            }

            if (!result.messages.isEmpty()) {
                batches.add(new ConsumerBatch(tp.topic(), tp.partition(), offset, result.messages));
            }
        }
        return batches;
    }

    /**
     * Legacy poll method — fetches from a specific partition without
     * group assignment enforcement. Prefer {@link #poll(int)} instead.
     *
     * @deprecated Use {@link #subscribe(String)} + {@link #poll(int)} for
     *             proper group coordination.
     */
    @Deprecated
    public ConsumerBatch pollPartition(String topic, int partition, int maxBytes) throws IOException {
        long offset = committedOffset(topic, partition);
        if (offset < 0) {
            offset = 0;
        }

        Protocol.FetchResult result = client.fetch(topic, partition, offset, maxBytes);
        if (result.hasError()) {
            throw new IOException(result.error);
        }

        return new ConsumerBatch(topic, partition, offset, result.messages);
    }

    public void commit(ConsumerBatch batch) throws IOException {
        commit(batch.topic(), batch.partition(), batch.nextOffset());
    }

    public void commit(String topic, int partition, long offset) throws IOException {
        Protocol.OffsetCommitResult result = client.commitOffset(groupId, topic, partition, offset);
        if (result.hasError()) {
            throw new IOException(result.error);
        }
    }

    public long committedOffset(String topic, int partition) throws IOException {
        Protocol.OffsetFetchResult result = client.fetchCommittedOffset(groupId, topic, partition);
        if (result.hasError()) {
            throw new IOException(result.error);
        }
        return result.offset;
    }

    /**
     * Returns the partitions currently assigned to this consumer.
     */
    public List<TopicPartition> assignedPartitions() {
        return assignedPartitions;
    }

    /**
     * Returns the current generation ID for this consumer's group membership.
     */
    public int generationId() {
        return generationId;
    }

    @Override
    public void close() throws IOException {
        try {
            if (memberId != null && !memberId.isEmpty()) {
                client.leaveGroup(groupId, memberId);
            }
        } finally {
            client.close();
        }
    }

    private void ensureSubscribed() {
        if (subscribedTopic == null || assignedPartitions.isEmpty()) {
            throw new IllegalStateException(
                "Not subscribed to any topic. Call subscribe() before poll()."
            );
        }
    }

    public static class ConsumerBatch {
        private final String topic;
        private final int partition;
        private final long startOffset;
        private final List<byte[]> messages;

        ConsumerBatch(String topic, int partition, long startOffset, List<byte[]> messages) {
            this.topic = topic;
            this.partition = partition;
            this.startOffset = startOffset;
            this.messages = messages;
        }

        public String topic() {
            return topic;
        }

        public int partition() {
            return partition;
        }

        public long startOffset() {
            return startOffset;
        }

        public long nextOffset() {
            return startOffset + messages.size();
        }

        public List<byte[]> messages() {
            return messages;
        }
    }
}
