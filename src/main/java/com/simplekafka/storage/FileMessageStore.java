package com.simplekafka.storage;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FileMessageStore implements MessageStore {
    private static final Pattern TOPIC_NAME = Pattern.compile("[A-Za-z0-9._-]+");
    private static final Pattern PARTITION_LOG = Pattern.compile("partition-(\\d+)\\.log");

    private final Path topicsRoot;
    private final Map<String, TopicLog> topics = new ConcurrentHashMap<>();
    private final ScheduledExecutorService flushExecutor;

    public FileMessageStore(Path dataRoot) {
        this.topicsRoot = dataRoot.resolve("topics");
        this.flushExecutor = Executors.newSingleThreadScheduledExecutor();
        try {
            Files.createDirectories(topicsRoot);
            loadExistingTopics();
        } catch (IOException e) {
            throw new IllegalStateException("Unable to initialize message store", e);
        }

        int flushMs = Integer.parseInt(System.getProperty("kafka.flush.ms", "100"));
        // Background flush every 'flushMs'
        this.flushExecutor.scheduleAtFixedRate(this::flushAll, flushMs, flushMs, TimeUnit.MILLISECONDS);
    }

    public void close() {
        flushExecutor.shutdown();
        try {
            if (!flushExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                flushExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            flushExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        for (TopicLog topic : topics.values()) {
            topic.close();
        }
    }

    private void flushAll() {
        for (TopicLog topic : topics.values()) {
            topic.flush();
        }
    }

    @Override
    public void createTopic(String name, int partitionCount) {
        validateTopic(name);
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("Partition count must be positive");
        }

        TopicLog topic = createTopicLog(name, partitionCount);
        TopicLog existing = topics.putIfAbsent(name, topic);
        if (existing != null) {
            throw new IllegalArgumentException("Topic already exists: " + name);
        }
    }

    @Override
    public long append(String topicName, int partition, byte[] message) {
        if (message == null) {
            throw new IllegalArgumentException("Message must not be null");
        }
        return topic(topicName).append(partition, message);
    }

    @Override
    public List<byte[]> read(String topicName, int partition, long offset, int maxBytes) {
        if (offset < 0) {
            throw new IllegalArgumentException("Offset must not be negative");
        }
        if (maxBytes < 0) {
            throw new IllegalArgumentException("Max bytes must not be negative");
        }
        return topic(topicName).read(partition, offset, maxBytes);
    }

    @Override
    public List<TopicMetadata> metadata() {
        List<String> names = new ArrayList<>(topics.keySet());
        Collections.sort(names);

        List<TopicMetadata> result = new ArrayList<>(names.size());
        for (String name : names) {
            TopicLog topic = topics.get(name);
            if (topic != null) {
                result.add(new TopicMetadata(name, topic.partitionCount()));
            }
        }
        return result;
    }

    @Override
    public boolean hasTopic(String name) {
        return topics.containsKey(name);
    }

    private void loadExistingTopics() throws IOException {
        try (var stream = Files.list(topicsRoot)) {
            List<Path> topicDirs = stream
                .filter(Files::isDirectory)
                .sorted()
                .toList();

            for (Path topicDir : topicDirs) {
                TopicLog topic = loadTopic(topicDir);
                if (topic.partitionCount() > 0) {
                    topics.put(topic.name(), topic);
                }
            }
        }
    }

    private TopicLog loadTopic(Path topicDir) throws IOException {
        List<Path> partitionFiles;
        try (var stream = Files.list(topicDir)) {
            partitionFiles = stream
                .filter(Files::isRegularFile)
                .filter(path -> PARTITION_LOG.matcher(path.getFileName().toString()).matches())
                .sorted(Comparator.comparingInt(FileMessageStore::partitionId))
                .toList();
        }

        List<PartitionLog> partitions = new ArrayList<>(partitionFiles.size());
        for (Path file : partitionFiles) {
            partitions.add(new PartitionLog(file));
        }
        return new TopicLog(topicDir.getFileName().toString(), partitions);
    }

    private TopicLog createTopicLog(String name, int partitionCount) {
        Path topicDir = topicsRoot.resolve(name);
        try {
            Files.createDirectories(topicDir);
            List<PartitionLog> partitions = new ArrayList<>(partitionCount);
            for (int i = 0; i < partitionCount; i++) {
                Path logFile = topicDir.resolve("partition-" + i + ".log");
                if (!Files.exists(logFile)) {
                    Files.createFile(logFile);
                }
                partitions.add(new PartitionLog(logFile));
            }
            return new TopicLog(name, partitions);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to create topic: " + name, e);
        }
    }

    private TopicLog topic(String name) {
        TopicLog topic = topics.get(name);
        if (topic == null) {
            throw new IllegalArgumentException("Unknown topic: " + name);
        }
        return topic;
    }

    private static void validateTopic(String name) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Topic name must not be empty");
        }
        if (!TOPIC_NAME.matcher(name).matches()) {
            throw new IllegalArgumentException("Invalid topic name: " + name);
        }
    }

    private static int partitionId(Path path) {
        Matcher matcher = PARTITION_LOG.matcher(path.getFileName().toString());
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid partition log file: " + path);
        }
        return Integer.parseInt(matcher.group(1));
    }

}
