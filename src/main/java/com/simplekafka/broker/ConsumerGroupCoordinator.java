package com.simplekafka.broker;

import com.simplekafka.common.PartitionAssignmentStrategy;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

class ConsumerGroupCoordinator {
    private final TopicPartitionProvider partitionProvider;
    private final PartitionAssignmentStrategy assignmentStrategy;
    private final Map<String, GroupState> groups = new ConcurrentHashMap<>();

    ConsumerGroupCoordinator(TopicPartitionProvider partitionProvider, PartitionAssignmentStrategy assignmentStrategy) {
        this.partitionProvider = partitionProvider;
        this.assignmentStrategy = assignmentStrategy;
    }

    JoinResult join(String groupId, String memberId, String topic) {
        validateGroup(groupId);
        validateTopic(topic);

        int partitionCount = partitionProvider.partitionCount(topic);
        if (memberId == null || memberId.isBlank()) {
            memberId = "member-" + UUID.randomUUID();
        }

        GroupState group = groups.computeIfAbsent(groupId, ignored -> new GroupState());
        synchronized (group) {
            group.members.put(memberId, new MemberState(topic));
            group.generationId++;

            List<TopicPartition> assignments = assignmentsFor(
                group, memberId, topic, partitionCount
            );

            // update each member's recorded generation so heartbeats work
            for (MemberState ms : group.members.values()) {
                ms.generation = group.generationId;
            }

            return new JoinResult(memberId, group.generationId, assignments);
        }
    }

    void heartbeat(String groupId, String memberId, int memberGeneration) {
        validateGroup(groupId);
        validateMember(memberId);

        GroupState group = groups.get(groupId);
        if (group == null) {
            throw new IllegalArgumentException("Unknown consumer group: " + groupId);
        }

        synchronized (group) {
            MemberState member = group.members.get(memberId);
            if (member == null) {
                throw new IllegalArgumentException("Unknown group member: " + memberId);
            }

            if (memberGeneration < group.generationId) {
                throw new RebalanceException("REBALANCE_IN_PROGRESS");
            }

            member.touch();
        }
    }

    void leave(String groupId, String memberId) {
        validateGroup(groupId);
        validateMember(memberId);

        GroupState group = groups.get(groupId);
        if (group == null) {
            throw new IllegalArgumentException("Unknown consumer group: " + groupId);
        }

        synchronized (group) {
            if (group.members.remove(memberId) == null) {
                throw new IllegalArgumentException("Unknown group member: " + memberId);
            }
            if (group.members.isEmpty()) {
                groups.remove(groupId, group);
            } else {
                group.generationId++;
            }
        }
    }

    /**
     * Assigns partitions to members using consistent hashing.
     * Each partition is assigned to exactly one member — enforcing
     * the "1 partition = 1 consumer per group" invariant.
     */
    private List<TopicPartition> assignmentsFor(
        GroupState group,
        String memberId,
        String topic,
        int partitionCount
    ) {
        // collect all members subscribed to this topic
        List<String> membersForTopic = new ArrayList<>();
        for (Map.Entry<String, MemberState> entry : group.members.entrySet()) {
            if (topic.equals(entry.getValue().topic)) {
                membersForTopic.add(entry.getKey());
            }
        }
        Collections.sort(membersForTopic);

        if (membersForTopic.isEmpty()) {
            return List.of();
        }

        // use consistent hashing to assign partitions
        Map<String, List<Integer>> allAssignments = assignmentStrategy.assign(membersForTopic, topic, partitionCount);

        // return only this member's partitions
        List<Integer> myPartitions = allAssignments.getOrDefault(memberId, List.of());
        List<TopicPartition> result = new ArrayList<>(myPartitions.size());
        for (int partition : myPartitions) {
            result.add(new TopicPartition(topic, partition));
        }
        return result;
    }

    private static void validateGroup(String groupId) {
        if (groupId == null || groupId.isBlank()) {
            throw new IllegalArgumentException("Consumer group id must not be empty");
        }
    }

    private static void validateMember(String memberId) {
        if (memberId == null || memberId.isBlank()) {
            throw new IllegalArgumentException("Member id must not be empty");
        }
    }

    private static void validateTopic(String topic) {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("Topic must not be empty");
        }
    }

    record TopicPartition(String topic, int partition) {}

    interface TopicPartitionProvider {
        int partitionCount(String topic);
    }

    record JoinResult(String memberId, int generationId, List<TopicPartition> assignments) {}

    private static class GroupState {
        private final Map<String, MemberState> members = new ConcurrentHashMap<>();
        private int generationId = 0;
    }

    private static class MemberState {
        private final String topic;
        private long lastHeartbeatMillis;
        private int generation;

        MemberState(String topic) {
            this.topic = topic;
            touch();
        }

        void touch() {
            lastHeartbeatMillis = System.currentTimeMillis();
        }
    }
}
