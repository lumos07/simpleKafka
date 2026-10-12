package com.simplekafka.broker;

import com.simplekafka.storage.TopicMetadata;

import com.simplekafka.protocol.Protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class BrokerStateTest {
    @TempDir
    Path tempDir;

    @Test
    void testProduceFetchAndOffsets() {
        BrokerState state = new BrokerState(tempDir);
        state.createTopic("orders", 2);

        byte[] first = "first".getBytes(StandardCharsets.UTF_8);
        byte[] second = "second".getBytes(StandardCharsets.UTF_8);

        assertEquals(0L, state.produce("orders", 0, first));
        assertEquals(1L, state.produce("orders", 0, second));
        assertEquals(0L, state.produce("orders", 1, "other".getBytes(StandardCharsets.UTF_8)));

        List<byte[]> messages = state.fetch("orders", 0, 0, 1024);
        assertEquals(2, messages.size());
        assertArrayEquals(first, messages.get(0));
        assertArrayEquals(second, messages.get(1));

        List<byte[]> fromSecondOffset = state.fetch("orders", 0, 1, 1024);
        assertEquals(1, fromSecondOffset.size());
        assertArrayEquals(second, fromSecondOffset.get(0));
    }

    @Test
    void testFetchHonorsMaxBytes() {
        BrokerState state = new BrokerState(tempDir);
        state.createTopic("events", 1);

        state.produce("events", 0, "abc".getBytes(StandardCharsets.UTF_8));
        state.produce("events", 0, "def".getBytes(StandardCharsets.UTF_8));

        List<byte[]> messages = state.fetch("events", 0, 0, 3);
        assertEquals(1, messages.size());
        assertArrayEquals("abc".getBytes(StandardCharsets.UTF_8), messages.get(0));
    }

    @Test
    void testMetadataIsStableAndSorted() {
        BrokerState state = new BrokerState(tempDir);
        state.createTopic("zeta", 1);
        state.createTopic("alpha", 3);

        List<TopicMetadata> metadata = state.metadata();
        assertEquals(2, metadata.size());
        assertEquals("alpha", metadata.get(0).name());
        assertEquals(3, metadata.get(0).partitionCount());
        assertEquals("zeta", metadata.get(1).name());
        assertEquals(1, metadata.get(1).partitionCount());
    }

    @Test
    void testInvalidStateOperationsFail() {
        BrokerState state = new BrokerState(tempDir);
        state.createTopic("orders", 1);

        assertThrows(IllegalArgumentException.class, () -> state.createTopic("orders", 1));
        assertThrows(IllegalArgumentException.class, () -> state.produce("missing", 0, new byte[] {1}));
        assertThrows(IllegalArgumentException.class, () -> state.produce("orders", 2, new byte[] {1}));
        assertThrows(IllegalArgumentException.class, () -> state.fetch("orders", 0, -1, 1024));
        assertFalse(state.hasTopic("missing"));
    }

    @Test
    void testMessagesAndMetadataPersistAcrossStateRestart() {
        BrokerState firstState = new BrokerState(tempDir);
        firstState.createTopic("payments", 2);
        firstState.produce("payments", 0, "paid-1".getBytes(StandardCharsets.UTF_8));
        firstState.produce("payments", 0, "paid-2".getBytes(StandardCharsets.UTF_8));

        Path partitionLog = tempDir.resolve("topics").resolve("payments").resolve("partition-0.log");
        assertTrue(Files.exists(partitionLog));

        BrokerState restartedState = new BrokerState(tempDir);
        List<TopicMetadata> metadata = restartedState.metadata();
        assertEquals(1, metadata.size());
        assertEquals("payments", metadata.get(0).name());
        assertEquals(2, metadata.get(0).partitionCount());

        List<byte[]> messages = restartedState.fetch("payments", 0, 0, 1024);
        assertEquals(2, messages.size());
        assertArrayEquals("paid-1".getBytes(StandardCharsets.UTF_8), messages.get(0));
        assertArrayEquals("paid-2".getBytes(StandardCharsets.UTF_8), messages.get(1));

        assertEquals(2L, restartedState.produce(
            "payments",
            0,
            "paid-3".getBytes(StandardCharsets.UTF_8)
        ));
    }

    @Test
    void testCommittedOffsetsPersistAcrossStateRestart() {
        BrokerState firstState = new BrokerState(tempDir);
        firstState.createTopic("shipments", 1);
        firstState.produce("shipments", 0, "shipment-1".getBytes(StandardCharsets.UTF_8));
        firstState.produce("shipments", 0, "shipment-2".getBytes(StandardCharsets.UTF_8));

        firstState.commitOffset("shipping-workers", "shipments", 0, 2L);

        Path offsetLog = tempDir
            .resolve("topics")
            .resolve("__consumer_offsets")
            .resolve("partition-0.log");
        assertTrue(Files.exists(offsetLog));

        BrokerState restartedState = new BrokerState(tempDir);
        assertEquals(2L, restartedState.fetchCommittedOffset("shipping-workers", "shipments", 0));

        List<TopicMetadata> metadata = restartedState.metadata();
        assertEquals(1, metadata.size());
        assertEquals("shipments", metadata.get(0).name());
    }

    @Test
    void testMissingCommittedOffsetReturnsNegativeOne() {
        BrokerState state = new BrokerState(tempDir);
        state.createTopic("shipments", 1);

        assertEquals(-1L, state.fetchCommittedOffset("shipping-workers", "shipments", 0));
    }
}
