package com.simplekafka.storage;

import java.util.List;

class TopicLog {

    private final String name;
    private final List<PartitionLog> partitions;

    TopicLog(String name, List<PartitionLog> partitions) {
        this.name = name;
        this.partitions = partitions;
    }

    String name() {
        return name;
    }

    int partitionCount() {
        return partitions.size();
    }

    long append(int partition, byte[] message) {
        return partition(partition).append(message);
    }

    List<byte[]> read(int partition, long offset, int maxBytes) {
        return partition(partition).read(offset, maxBytes);
    }

    private PartitionLog partition(int partition) {
        if (partition < 0 || partition >= partitions.size()) {
            throw new IllegalArgumentException(
                "Invalid partition " + partition + " for topic " + name
            );
        }
        return partitions.get(partition);
    }

    void close() {
        for (PartitionLog p : partitions) {
            p.close();
        }
    }

    void flush() {
        for (PartitionLog p : partitions) {
            p.flush();
        }
    }
}

