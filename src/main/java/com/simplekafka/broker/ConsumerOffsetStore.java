package com.simplekafka.broker;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.simplekafka.storage.MessageStore;

class ConsumerOffsetStore {
    static final String INTERNAL_TOPIC = "__consumer_offsets";

    private final MessageStore messageStore;
    private final Map<String, Long> committedOffsets = new ConcurrentHashMap<>();

    ConsumerOffsetStore(MessageStore messageStore) {
        this.messageStore = messageStore;
        if (!messageStore.hasTopic(INTERNAL_TOPIC)) {
            messageStore.createTopic(INTERNAL_TOPIC, 1);
        }
        loadCommittedOffsets();
    }

    void commit(String groupId, String topic, int partition, long offset) {
        validateGroup(groupId);
        if (offset < 0) {
            throw new IllegalArgumentException("Committed offset must not be negative");
        }

        validateTopicPartition(topic, partition);
        byte[] record = encodeRecord(groupId, topic, partition, offset);
        synchronized (this) {
            messageStore.append(INTERNAL_TOPIC, 0, record);
            committedOffsets.put(key(groupId, topic, partition), offset);
        }
    }

    long fetch(String groupId, String topic, int partition) {
        validateGroup(groupId);
        validateTopicPartition(topic, partition);
        return committedOffsets.getOrDefault(key(groupId, topic, partition), -1L);
    }

    private void loadCommittedOffsets() {
        List<byte[]> records = messageStore.read(INTERNAL_TOPIC, 0, 0, Integer.MAX_VALUE);
        for (byte[] record : records) {
            OffsetRecord decoded = decodeRecord(record);
            committedOffsets.put(key(decoded.groupId(), decoded.topic(), decoded.partition()), decoded.offset());
        }
    }

    private void validateTopicPartition(String topic, int partition) {
        if (INTERNAL_TOPIC.equals(topic)) {
            throw new IllegalArgumentException("Cannot commit offsets for internal topic");
        }
        messageStore.read(topic, partition, 0, 0);
    }

    private static void validateGroup(String groupId) {
        if (groupId == null || groupId.isBlank()) {
            throw new IllegalArgumentException("Consumer group id must not be empty");
        }
    }

    private static byte[] encodeRecord(String groupId, String topic, int partition, long offset) {
        byte[] groupBytes = groupId.getBytes(StandardCharsets.UTF_8);
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        int capacity = 2 + groupBytes.length + 2 + topicBytes.length + 4 + 8;

        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        buffer.putShort((short)groupBytes.length);
        buffer.put(groupBytes);
        buffer.putShort((short)topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(partition);
        buffer.putLong(offset);
        return buffer.array();
    }

    private static OffsetRecord decodeRecord(byte[] record) {
        ByteBuffer buffer = ByteBuffer.wrap(record);
        String groupId = extractString(buffer);
        String topic = extractString(buffer);
        int partition = buffer.getInt();
        long offset = buffer.getLong();
        return new OffsetRecord(groupId, topic, partition, offset);
    }

    private static String extractString(ByteBuffer buffer) {
        int length = Short.toUnsignedInt(buffer.getShort());
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static String key(String groupId, String topic, int partition) {
        return groupId + "\u0000" + topic + "\u0000" + partition;
    }

    private record OffsetRecord(String groupId, String topic, int partition, long offset) {}
}
