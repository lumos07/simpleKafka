package com.simplekafka.common;

import java.util.List;
import java.util.Map;

@FunctionalInterface
public interface PartitionAssignmentStrategy {
    Map<String, List<Integer>> assign(List<String> members, String topic, int partitionCount);
}
