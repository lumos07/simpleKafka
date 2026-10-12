package com.simplekafka.common;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * A consistent hash ring that assigns partitions to consumer members
 * with minimal reassignment when members join or leave.
 *
 * Each member gets a configurable number of virtual nodes spread across
 * the ring to ensure even distribution.
 */
public class ConsistentHashRing {
    private static final int DEFAULT_VIRTUAL_NODES = 100;

    private final TreeMap<Long, String> ring = new TreeMap<>();
    private final int virtualNodes;

    public ConsistentHashRing(List<String> members) {
        this(members, DEFAULT_VIRTUAL_NODES);
    }

    public ConsistentHashRing(List<String> members, int virtualNodes) {
        if (virtualNodes <= 0) {
            throw new IllegalArgumentException("Virtual node count must be positive");
        }
        this.virtualNodes = virtualNodes;
        for (String member : members) {
            addMember(member);
        }
    }

    /**
     * Returns the member that owns the given partition.
     */
    public String assign(String topic, int partition) {
        if (ring.isEmpty()) {
            throw new IllegalStateException("No members on the ring");
        }

        long hash = hash(topic + "-" + partition);
        Map.Entry<Long, String> entry = ring.ceilingEntry(hash);
        if (entry == null) {
            // wrap around to the first node
            entry = ring.firstEntry();
        }
        return entry.getValue();
    }

    /**
     * Returns partition assignments for all members for the given topic.
     * Each partition is assigned to exactly one member.
     */
    public Map<String, List<Integer>> assignAll(String topic, int partitionCount) {
        Map<String, List<Integer>> assignments = new HashMap<>();
        for (int partition = 0; partition < partitionCount; partition++) {
            String member = assign(topic, partition);
            assignments.computeIfAbsent(member, k -> new ArrayList<>()).add(partition);
        }
        // sort partition lists for deterministic output
        for (List<Integer> partitions : assignments.values()) {
            Collections.sort(partitions);
        }
        return assignments;
    }

    private void addMember(String member) {
        for (int i = 0; i < virtualNodes; i++) {
            long hash = hash(member + "-vn-" + i);
            ring.put(hash, member);
        }
    }

    /**
     * Hash using SHA-256, truncated to a long for placement on the ring.
     * SHA-256 provides much better distribution than Object.hashCode().
     */
    static long hash(String key) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(key.getBytes(StandardCharsets.UTF_8));
            // take the first 8 bytes as a long
            long h = 0;
            for (int i = 0; i < 8; i++) {
                h = (h << 8) | (digest[i] & 0xFF);
            }
            return h;
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is guaranteed to be available in every JVM
            throw new AssertionError("SHA-256 not available", e);
        }
    }
}
