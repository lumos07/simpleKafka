package com.simplekafka.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

public class ConsistentHashRingTest {

    @Test
    void singleMemberGetsAllPartitions() {
        ConsistentHashRing ring = new ConsistentHashRing(List.of("member-A"));
        Map<String, List<Integer>> assignments = ring.assignAll("orders", 6);

        assertEquals(1, assignments.size());
        assertTrue(assignments.containsKey("member-A"));
        assertEquals(List.of(0, 1, 2, 3, 4, 5), assignments.get("member-A"));
    }

    @Test
    void allPartitionsAssigned() {
        ConsistentHashRing ring = new ConsistentHashRing(List.of("A", "B", "C"));
        Map<String, List<Integer>> assignments = ring.assignAll("orders", 10);

        Set<Integer> allPartitions = new HashSet<>();
        for (List<Integer> parts : assignments.values()) {
            allPartitions.addAll(parts);
        }
        assertEquals(10, allPartitions.size());
        for (int i = 0; i < 10; i++) {
            assertTrue(allPartitions.contains(i), "Missing partition " + i);
        }
    }

    @Test
    void noPartitionAssignedToMultipleMembers() {
        ConsistentHashRing ring = new ConsistentHashRing(List.of("A", "B", "C"));
        Map<String, List<Integer>> assignments = ring.assignAll("orders", 20);

        int totalAssigned = 0;
        for (List<Integer> parts : assignments.values()) {
            totalAssigned += parts.size();
        }
        // if no overlap, total should equal partition count
        assertEquals(20, totalAssigned);
    }

    @Test
    void reasonablyEvenDistribution() {
        ConsistentHashRing ring = new ConsistentHashRing(List.of("A", "B", "C"));
        Map<String, List<Integer>> assignments = ring.assignAll("orders", 99);

        // with 99 partitions and 3 members, ideal is 33 each
        // allow generous tolerance (±20) since it's hash-based
        for (Map.Entry<String, List<Integer>> entry : assignments.entrySet()) {
            int count = entry.getValue().size();
            assertTrue(count >= 13, entry.getKey() + " got too few: " + count);
            assertTrue(count <= 53, entry.getKey() + " got too many: " + count);
        }
    }

    @Test
    void deterministic() {
        ConsistentHashRing ring1 = new ConsistentHashRing(List.of("A", "B", "C"));
        ConsistentHashRing ring2 = new ConsistentHashRing(List.of("A", "B", "C"));

        for (int i = 0; i < 20; i++) {
            assertEquals(ring1.assign("orders", i), ring2.assign("orders", i),
                "Non-deterministic assignment for partition " + i);
        }
    }

    @Test
    void minimalMovementOnMemberAddition() {
        ConsistentHashRing before = new ConsistentHashRing(List.of("A", "B", "C"));
        ConsistentHashRing after = new ConsistentHashRing(List.of("A", "B", "C", "D"));

        int partitionCount = 100;
        int moved = 0;
        for (int i = 0; i < partitionCount; i++) {
            if (!before.assign("orders", i).equals(after.assign("orders", i))) {
                moved++;
            }
        }

        // with consistent hashing, ~25% should move (100/4 = 25)
        // allow generous tolerance: at most 50% moved
        assertTrue(moved <= 50,
            "Too many partitions moved: " + moved + "/" + partitionCount);
        assertTrue(moved > 0, "No partitions moved — D got nothing");
    }

    @Test
    void minimalMovementOnMemberRemoval() {
        ConsistentHashRing before = new ConsistentHashRing(List.of("A", "B", "C", "D"));
        ConsistentHashRing after = new ConsistentHashRing(List.of("A", "B", "C"));

        int partitionCount = 100;
        int moved = 0;
        for (int i = 0; i < partitionCount; i++) {
            String ownerBefore = before.assign("orders", i);
            String ownerAfter = after.assign("orders", i);
            // only D's partitions should move
            if (!ownerBefore.equals(ownerAfter)) {
                assertEquals("D", ownerBefore,
                    "Partition " + i + " moved from " + ownerBefore + " to " + ownerAfter +
                    " — should only move partitions that belonged to D");
                moved++;
            }
        }

        assertTrue(moved > 0, "D had partitions that should have been redistributed");
    }

    @Test
    void differentTopicsDifferentAssignments() {
        ConsistentHashRing ring = new ConsistentHashRing(List.of("A", "B"));

        // same partition number but different topics should not always
        // map to the same member
        boolean anyDifferent = false;
        for (int i = 0; i < 20; i++) {
            if (!ring.assign("orders", i).equals(ring.assign("payments", i))) {
                anyDifferent = true;
                break;
            }
        }
        assertTrue(anyDifferent,
            "All partitions mapped identically across topics — hash should include topic");
    }
}
