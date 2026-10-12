package com.simplekafka.common;

import java.util.List;
import java.util.Map;

public class ConsistentHashAssignmentStrategy implements PartitionAssignmentStrategy {
    @Override
    public Map<String, List<Integer>> assign(List<String> members, String topic, int partitionCount) {
        ConsistentHashRing ring = new ConsistentHashRing(members);
        return ring.assignAll(topic, partitionCount);
    }
}
