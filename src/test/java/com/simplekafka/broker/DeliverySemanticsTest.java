package com.simplekafka.broker;

import com.simplekafka.protocol.Protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.simplekafka.client.SimpleKafkaClient;
import com.simplekafka.client.SimpleKafkaConsumer;

/**
 * These tests PROVE the delivery semantics problems in the current system.
 * They are designed to run BEFORE the idempotent producer fix,
 * demonstrating exactly WHY the fix is needed.
 */
@SuppressWarnings("deprecation")  // pollPartition is used intentionally to test raw partition semantics
public class DeliverySemanticsTest {

    @TempDir
    Path tempDir;

    private SimpleKafkaBroker broker;
    private int port;

    @BeforeEach
    void setUp() throws IOException {
        broker = BrokerFactory.create(0, tempDir);
        broker.start();
        port = broker.getPort();

        // create a topic for our tests
        try (SimpleKafkaClient client = new SimpleKafkaClient("localhost", port)) {
            Protocol.CreateTopicResult result = client.createTopic("payments", 1, (short) 1);
            assertFalse(result.hasError());
        }
    }

    @AfterEach
    void tearDown() throws IOException {
        broker.close();
    }

    // =========================================================================
    // PRODUCER DUPLICATE PROBLEM — proves why we need idempotent producer
    // =========================================================================

    @Test
    @DisplayName("BUG: Producer retry writes the same message twice — no dedup")
    void producerRetryCausesDuplicate() throws IOException {
        byte[] paymentMsg = "charge customer $100".getBytes(StandardCharsets.UTF_8);

        try (SimpleKafkaClient producer = new SimpleKafkaClient("localhost", port)) {
            // First attempt: message sent and acked
            Protocol.ProduceResult firstAttempt = producer.produce("payments", 0, paymentMsg);
            assertFalse(firstAttempt.hasError());
            long firstOffset = firstAttempt.offset;
            assertEquals(0L, firstOffset);

            // Simulate retry: producer thinks first attempt failed (ack lost in network)
            // so it sends the EXACT SAME message again
            Protocol.ProduceResult retryAttempt = producer.produce("payments", 0, paymentMsg);
            assertFalse(retryAttempt.hasError());
            long retryOffset = retryAttempt.offset;

            // *** THE BUG: both writes succeeded at DIFFERENT offsets ***
            // The broker blindly appended the message twice
            assertEquals(1L, retryOffset, 
                "Retry got offset 1 — broker wrote the message TWICE!");
            
            // Proof: if we fetch, we get 2 copies of the same message
            Protocol.FetchResult fetched = producer.fetch("payments", 0, 0, 4096);
            assertFalse(fetched.hasError());
            assertEquals(2, fetched.messages.size(),
                "Two copies of the same payment message on disk!");
            assertArrayEquals(paymentMsg, fetched.messages.get(0));
            assertArrayEquals(paymentMsg, fetched.messages.get(1));

            // IMPACT: A consumer processing these messages will charge the
            // customer $100 TWICE. This is why we need the idempotent producer.
        }
    }

    @Test
    @DisplayName("BUG: Multiple retries = multiple duplicates — unbounded")
    void multipleRetriesCreateMultipleDuplicates() throws IOException {
        byte[] msg = "debit account #42".getBytes(StandardCharsets.UTF_8);

        try (SimpleKafkaClient producer = new SimpleKafkaClient("localhost", port)) {
            // Simulate a producer that retries 5 times (aggressive retry policy)
            for (int attempt = 0; attempt < 5; attempt++) {
                Protocol.ProduceResult result = producer.produce("payments", 0, msg);
                assertFalse(result.hasError());
                assertEquals((long) attempt, result.offset,
                    "Attempt " + attempt + " got a new offset — duplicate written!");
            }

            // 5 copies of the same debit on disk
            Protocol.FetchResult fetched = producer.fetch("payments", 0, 0, 65536);
            assertEquals(5, fetched.messages.size(),
                "5 retries = 5 copies of the same debit. Customer charged 5x!");
        }
    }

    // =========================================================================
    // CONSUMER AT-LEAST-ONCE — proves the commit-after-process model
    // =========================================================================

    @Test
    @DisplayName("At-Least-Once: uncommitted messages are redelivered on reconnect")
    void uncommittedMessagesRedelivered() throws IOException {
        // Produce 3 messages
        try (SimpleKafkaClient producer = new SimpleKafkaClient("localhost", port)) {
            producer.produce("payments", 0, "msg-1".getBytes(StandardCharsets.UTF_8));
            producer.produce("payments", 0, "msg-2".getBytes(StandardCharsets.UTF_8));
            producer.produce("payments", 0, "msg-3".getBytes(StandardCharsets.UTF_8));
        }

        // Consumer 1: reads all 3 messages, "processes" them, but CRASHES before commit
        try (SimpleKafkaConsumer consumer1 = new SimpleKafkaConsumer("localhost", port, "payment-workers")) {
            SimpleKafkaConsumer.ConsumerBatch batch = consumer1.pollPartition("payments", 0, 4096);
            assertEquals(3, batch.messages().size());
            assertEquals(0L, batch.startOffset());

            // process() happens here...
            // consumer1.commit(batch);  ← CRASH! Never called.
        }
        // consumer1 is closed (simulating crash/restart)

        // Consumer 2: same group, reconnects after "crash"
        // Since offset was never committed, it should get ALL 3 messages again
        try (SimpleKafkaConsumer consumer2 = new SimpleKafkaConsumer("localhost", port, "payment-workers")) {
            SimpleKafkaConsumer.ConsumerBatch rebatch = consumer2.pollPartition("payments", 0, 4096);
            assertEquals(0L, rebatch.startOffset(),
                "Offset is still 0 — messages redelivered because commit never happened!");
            assertEquals(3, rebatch.messages().size(),
                "All 3 messages redelivered — at-least-once semantics in action");
        }
    }

    @Test
    @DisplayName("At-Least-Once: committed offset survives reconnect — no redelivery")
    void committedMessagesNotRedelivered() throws IOException {
        // Produce 3 messages
        try (SimpleKafkaClient producer = new SimpleKafkaClient("localhost", port)) {
            producer.produce("payments", 0, "msg-1".getBytes(StandardCharsets.UTF_8));
            producer.produce("payments", 0, "msg-2".getBytes(StandardCharsets.UTF_8));
            producer.produce("payments", 0, "msg-3".getBytes(StandardCharsets.UTF_8));
        }

        // Consumer 1: reads all 3 messages, processes them, AND commits
        try (SimpleKafkaConsumer consumer1 = new SimpleKafkaConsumer("localhost", port, "payment-workers")) {
            SimpleKafkaConsumer.ConsumerBatch batch = consumer1.pollPartition("payments", 0, 4096);
            assertEquals(3, batch.messages().size());

            // process() + commit() — the happy path
            consumer1.commit(batch);
            assertEquals(3L, consumer1.committedOffset("payments", 0));
        }

        // Consumer 2: same group, reconnects
        // Committed offset = 3, so it should only get NEW messages (none here)
        try (SimpleKafkaConsumer consumer2 = new SimpleKafkaConsumer("localhost", port, "payment-workers")) {
            SimpleKafkaConsumer.ConsumerBatch batch2 = consumer2.pollPartition("payments", 0, 4096);
            assertEquals(3L, batch2.startOffset(),
                "Starts from committed offset 3 — no redelivery");
            assertEquals(0, batch2.messages().size(),
                "No new messages — previously committed ones are not redelivered");
        }
    }

    @Test
    @DisplayName("At-Least-Once: partial commit = partial redelivery")
    void partialCommitPartialRedelivery() throws IOException {
        // Produce 5 messages
        try (SimpleKafkaClient producer = new SimpleKafkaClient("localhost", port)) {
            for (int i = 0; i < 5; i++) {
                producer.produce("payments", 0,
                    ("payment-" + i).getBytes(StandardCharsets.UTF_8));
            }
        }

        // Consumer 1: reads 5 messages, processes first 2, commits offset=2, then crashes
        try (SimpleKafkaConsumer consumer1 = new SimpleKafkaConsumer("localhost", port, "payment-workers")) {
            SimpleKafkaConsumer.ConsumerBatch batch = consumer1.pollPartition("payments", 0, 65536);
            assertEquals(5, batch.messages().size());

            // Only commit through offset 2 (processed first 2)
            consumer1.commit("payments", 0, 2L);
            // "crash" before processing messages at offsets 2, 3, 4
        }

        // Consumer 2: reconnects. Should get messages 2, 3, 4 (the uncommitted ones)
        try (SimpleKafkaConsumer consumer2 = new SimpleKafkaConsumer("localhost", port, "payment-workers")) {
            SimpleKafkaConsumer.ConsumerBatch rebatch = consumer2.pollPartition("payments", 0, 65536);
            assertEquals(2L, rebatch.startOffset(),
                "Resumes from committed offset 2");
            assertEquals(3, rebatch.messages().size(),
                "3 messages redelivered (offsets 2, 3, 4)");
            assertEquals("payment-2",
                new String(rebatch.messages().get(0), StandardCharsets.UTF_8),
                "First redelivered message is payment-2");
        }
    }

    // =========================================================================
    // WHY AT-MOST-ONCE IS DANGEROUS — demonstrates data loss
    // =========================================================================

    @Test
    @DisplayName("DANGER: At-Most-Once pattern loses messages on crash")
    void atMostOncePatternLosesMessages() throws IOException {
        // Produce 3 messages
        try (SimpleKafkaClient producer = new SimpleKafkaClient("localhost", port)) {
            producer.produce("payments", 0, "critical-1".getBytes(StandardCharsets.UTF_8));
            producer.produce("payments", 0, "critical-2".getBytes(StandardCharsets.UTF_8));
            producer.produce("payments", 0, "critical-3".getBytes(StandardCharsets.UTF_8));
        }

        // Simulate at-most-once: commit BEFORE processing
        try (SimpleKafkaConsumer consumer1 = new SimpleKafkaConsumer("localhost", port, "payment-workers")) {
            SimpleKafkaConsumer.ConsumerBatch batch = consumer1.pollPartition("payments", 0, 4096);
            assertEquals(3, batch.messages().size());

            // At-most-once: commit FIRST
            consumer1.commit(batch);

            // "crash" happens here — messages were never processed!
            // But the offset is already at 3.
        }

        // Consumer reconnects — offset is at 3, no messages to read
        try (SimpleKafkaConsumer consumer2 = new SimpleKafkaConsumer("localhost", port, "payment-workers")) {
            SimpleKafkaConsumer.ConsumerBatch batch2 = consumer2.pollPartition("payments", 0, 4096);
            assertEquals(3L, batch2.startOffset());
            assertEquals(0, batch2.messages().size(),
                "MESSAGES LOST FOREVER! Committed offset moved past unprocessed messages.");

            // The 3 critical payment messages are gone. No way to recover.
            // This is why at-most-once is unacceptable for financial data.
        }
    }

    // =========================================================================
    // THE FIX: IDEMPOTENT PRODUCER (deduplication)
    // =========================================================================

    @Test
    @DisplayName("FIX: Idempotent Producer prevents duplicates on exact retry")
    void idempotentProducerPreventsDuplicates() throws IOException {
        byte[] paymentMsg = "charge customer $100 (idempotent)".getBytes(StandardCharsets.UTF_8);

        try (SimpleKafkaClient producer = new SimpleKafkaClient("localhost", port)) {
            // 1. Initialize Producer ID
            Protocol.InitProducerIdResult init = producer.initProducerId();
            assertFalse(init.hasError());
            long pid = init.producerId;

            // 2. First attempt (sequence = 0)
            Protocol.ProduceResult firstAttempt = producer.produce(pid, 0, "payments", 0, paymentMsg);
            assertFalse(firstAttempt.hasError());
            long firstOffset = firstAttempt.offset;
            assertEquals(0L, firstOffset);
            assertFalse(firstAttempt.duplicate);

            // 3. Simulate network timeout + retry (sequence = 0 again)
            Protocol.ProduceResult retryAttempt = producer.produce(pid, 0, "payments", 0, paymentMsg);
            assertFalse(retryAttempt.hasError());
            
            // Verification: Offset should be the SAME, duplicate flag should be TRUE
            assertEquals(0L, retryAttempt.offset, "Offset should not increase on retry");
            assertTrue(retryAttempt.duplicate, "Broker should flag this as a duplicate");

            // Proof: fetch and verify only 1 message exists on disk
            Protocol.FetchResult fetched = producer.fetch("payments", 0, 0, 4096);
            assertFalse(fetched.hasError());
            assertEquals(1, fetched.messages.size(), "Only ONE copy of the payment on disk!");
        }
    }

    @Test
    @DisplayName("FIX: Idempotent Producer detects lost messages (sequence gaps)")
    void idempotentProducerDetectsGaps() throws IOException {
        try (SimpleKafkaClient producer = new SimpleKafkaClient("localhost", port)) {
            long pid = producer.initProducerId().producerId;

            // Seq 0: success
            Protocol.ProduceResult msg0 = producer.produce(pid, 0, "payments", 0, "msg0".getBytes(StandardCharsets.UTF_8));
            assertFalse(msg0.hasError());

            // Seq 2 (skipping 1): should fail due to gap
            Protocol.ProduceResult msg2 = producer.produce(pid, 2, "payments", 0, "msg2".getBytes(StandardCharsets.UTF_8));
            assertTrue(msg2.hasError(), "Expected error due to sequence gap");
            assertTrue(msg2.error.contains("gap"), "Error message should mention gap");
        }
    }
}
