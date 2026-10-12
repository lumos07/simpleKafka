package com.simplekafka.storage;

import java.util.List;

public interface MessageStore extends AutoCloseable {
    void createTopic(String name, int partitionCount);

    long append(String topicName, int partition, byte[] message);

    List<byte[]> read(String topicName, int partition, long offset, int maxBytes);

    List<TopicMetadata> metadata();

    boolean hasTopic(String name);

    @Override
    void close() throws Exception;
}
