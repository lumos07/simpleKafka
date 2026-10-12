package com.simplekafka.broker;


import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import com.simplekafka.storage.FileMessageStore;
import com.simplekafka.storage.MessageStore;
import com.simplekafka.storage.TopicMetadata;

public class BrokerState {
    private static final Path DEFAULT_DATA_DIR = Path.of("data");

    private final MessageStore messageStore;
    private final ConsumerOffsetStore offsetStore;

    public BrokerState() {
        this(DEFAULT_DATA_DIR);
    }

    public BrokerState(Path dataDir) {
        this(new FileMessageStore(dataDir));
    }

    public BrokerState(MessageStore messageStore) {
        this(messageStore, new ConsumerOffsetStore(messageStore));
    }

    BrokerState(MessageStore messageStore, ConsumerOffsetStore offsetStore) {
        this.messageStore = messageStore;
        this.offsetStore = offsetStore;
    }

    public void createTopic(String name, int partitionCount) {
        messageStore.createTopic(name, partitionCount);
    }

    public long produce(String topicName, int partition, byte[] message) {
        return messageStore.append(topicName, partition, message);
    }

    public List<byte[]> fetch(String topicName, int partition, long offset, int maxBytes) {
        return messageStore.read(topicName, partition, offset, maxBytes);
    }

    public List<TopicMetadata> metadata() {
        List<TopicMetadata> topics = messageStore.metadata();
        List<TopicMetadata> result = new ArrayList<>(topics.size());
        for (TopicMetadata topic : topics) {
            if (ConsumerOffsetStore.INTERNAL_TOPIC.equals(topic.name())) {
                continue;
            }
            result.add(topic);
        }
        return result;
    }

    public boolean hasTopic(String name) {
        return messageStore.hasTopic(name);
    }

    public void commitOffset(String groupId, String topicName, int partition, long offset) {
        offsetStore.commit(groupId, topicName, partition, offset);
    }

    public long fetchCommittedOffset(String groupId, String topicName, int partition) {
        return offsetStore.fetch(groupId, topicName, partition);
    }
    public void close() {
        try {
            messageStore.close();
        } catch (Exception e) {
            System.err.println("Failed to close MessageStore: " + e.getMessage());
        }
    }
}
