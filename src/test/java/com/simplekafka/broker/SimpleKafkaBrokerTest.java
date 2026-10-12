package com.simplekafka.broker;

import com.simplekafka.protocol.Protocol;
import com.simplekafka.protocol.Frame;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.simplekafka.client.SimpleKafkaClient;
import com.simplekafka.client.SimpleKafkaConsumer;

public class SimpleKafkaBrokerTest {
    @TempDir
    Path tempDir;

    @Test
    void testDirectRequestHandling() throws Exception {
        try (SimpleKafkaBroker broker = BrokerFactory.create(0, tempDir)) {
            Protocol.CreateTopicResult createResult = Protocol.decodeCreateTopicResponse(
                broker.handleRequest(Protocol.encodeCreateTopicRequest("orders", 2, (short)1))
            );
            assertFalse(createResult.hasError());

            Protocol.ProduceResult produceResult = Protocol.decodeProduceResponse(
                broker.handleRequest(Protocol.encodeProducerequest(
                    "orders",
                    1,
                    "confirmed".getBytes(StandardCharsets.UTF_8)
                ))
            );
            assertFalse(produceResult.hasError());
            assertEquals(0L, produceResult.offset);

            Protocol.FetchResult fetchResult = Protocol.decodeFetchResponse(
                broker.handleRequest(Protocol.encodeFetchrequest("orders", 1, 0, 1024))
            );
            assertFalse(fetchResult.hasError());
            assertEquals(1, fetchResult.messages.size());
            assertEquals("confirmed", new String(fetchResult.messages.get(0), StandardCharsets.UTF_8));

            Protocol.MetaDataResult metadataResult = Protocol.decodeMetadataResponse(
                broker.handleRequest(Protocol.encodeMetadatarequest())
            );
            assertFalse(metadataResult.hasError());
            assertEquals(1, metadataResult.topics.size());
            assertEquals("orders", metadataResult.topics.get(0).topic_name);
        }
    }

    @Test
    void testInvalidRequestReturnsErrorResponse() throws Exception {
        try (SimpleKafkaBroker broker = BrokerFactory.create(0, tempDir)) {
            Protocol.ProduceResult result = Protocol.decodeProduceResponse(
                broker.handleRequest(Protocol.encodeProducerequest(
                    "missing",
                    0,
                    "message".getBytes(StandardCharsets.UTF_8)
                ))
            );

            assertTrue(result.hasError());
        }
    }

    @Test
    void testTcpServerHandlesConcurrentProducerRequests() throws Exception {
        SimpleKafkaBroker broker = BrokerFactory.create(0, tempDir);
        broker.start();

        try {
            int port = broker.getPort();
            Protocol.CreateTopicResult createResult = Protocol.decodeCreateTopicResponse(
                send(port, Protocol.encodeCreateTopicRequest("events", 1, (short)1))
            );
            assertFalse(createResult.hasError());

            int producers = 20;
            ExecutorService executor = Executors.newFixedThreadPool(4);
            List<Callable<Long>> tasks = new ArrayList<>();
            for (int i = 0; i < producers; i++) {
                final int producerId = i;
                tasks.add(() -> {
                    ByteBuffer response = send(
                        port,
                        Protocol.encodeProducerequest(
                            "events",
                            0,
                            ("message-" + producerId).getBytes(StandardCharsets.UTF_8)
                        )
                    );
                    Protocol.ProduceResult result = Protocol.decodeProduceResponse(response);
                    if (result.hasError()) {
                        throw new AssertionError(result.error);
                    }
                    return result.offset;
                });
            }

            List<Future<Long>> futures = executor.invokeAll(tasks);
            executor.shutdown();

            Set<Long> offsets = new HashSet<>();
            for (Future<Long> future : futures) {
                offsets.add(future.get());
            }
            assertEquals(producers, offsets.size());

            Protocol.FetchResult fetchResult = Protocol.decodeFetchResponse(
                send(port, Protocol.encodeFetchrequest("events", 0, 0, 1024 * 1024))
            );
            assertFalse(fetchResult.hasError());
            assertEquals(producers, fetchResult.messages.size());
        } finally {
            broker.close();
        }
    }

    @Test
    void testMultiplePersistentClientsProduceAndFetchConcurrently() throws Exception {
        SimpleKafkaBroker broker = BrokerFactory.create(0, tempDir);
        broker.start();

        try {
            int port = broker.getPort();
            try (SimpleKafkaClient admin = new SimpleKafkaClient("localhost", port)) {
                Protocol.CreateTopicResult createResult = admin.createTopic("metrics", 2, (short)1);
                assertFalse(createResult.hasError());
            }

            int producerClients = 8;
            int messagesPerClient = 25;
            CountDownLatch startSignal = new CountDownLatch(1);
            ExecutorService producers = Executors.newFixedThreadPool(producerClients);
            List<Callable<Integer>> produceTasks = new ArrayList<>();

            for (int clientId = 0; clientId < producerClients; clientId++) {
                final int id = clientId;
                produceTasks.add(() -> {
                    startSignal.await();
                    int partition = id % 2;
                    try (SimpleKafkaClient client = new SimpleKafkaClient("localhost", port)) {
                        for (int i = 0; i < messagesPerClient; i++) {
                            Protocol.ProduceResult result = client.produce(
                                "metrics",
                                partition,
                                ("client-" + id + "-message-" + i).getBytes(StandardCharsets.UTF_8)
                            );
                            if (result.hasError()) {
                                throw new AssertionError(result.error);
                            }
                        }
                    }
                    return messagesPerClient;
                });
            }

            List<Future<Integer>> produceFutures = new ArrayList<>();
            for (Callable<Integer> task : produceTasks) {
                produceFutures.add(producers.submit(task));
            }
            startSignal.countDown();

            int produced = 0;
            for (Future<Integer> future : produceFutures) {
                produced += future.get();
            }
            producers.shutdown();
            assertEquals(producerClients * messagesPerClient, produced);

            ExecutorService consumers = Executors.newFixedThreadPool(2);
            Future<Integer> firstPartitionCount = consumers.submit(
                () -> fetchMessageCount(port, "metrics", 0)
            );
            Future<Integer> secondPartitionCount = consumers.submit(
                () -> fetchMessageCount(port, "metrics", 1)
            );

            int firstPartitionExpected = (producerClients / 2) * messagesPerClient;
            int secondPartitionExpected = (producerClients / 2) * messagesPerClient;
            assertEquals(firstPartitionExpected, firstPartitionCount.get());
            assertEquals(secondPartitionExpected, secondPartitionCount.get());
            consumers.shutdown();
        } finally {
            broker.close();
        }
    }

    @Test
    void testSimultaneousProduceAndFetchAcrossClients() throws Exception {
        SimpleKafkaBroker broker = BrokerFactory.create(0, tempDir);
        broker.start();

        try {
            int port = broker.getPort();
            try (SimpleKafkaClient admin = new SimpleKafkaClient("localhost", port)) {
                Protocol.CreateTopicResult createResult = admin.createTopic("stream", 1, (short)1);
                assertFalse(createResult.hasError());
            }

            int producerClients = 4;
            int messagesPerProducer = 30;
            int expectedMessages = producerClients * messagesPerProducer;
            CountDownLatch startSignal = new CountDownLatch(1);
            ExecutorService executor = Executors.newFixedThreadPool(producerClients + 2);
            List<Future<Integer>> producerFutures = new ArrayList<>();

            for (int producerId = 0; producerId < producerClients; producerId++) {
                final int id = producerId;
                producerFutures.add(executor.submit(() -> {
                    startSignal.await();
                    try (SimpleKafkaClient client = new SimpleKafkaClient("localhost", port)) {
                        for (int i = 0; i < messagesPerProducer; i++) {
                            Protocol.ProduceResult result = client.produce(
                                "stream",
                                0,
                                ("producer-" + id + "-message-" + i).getBytes(StandardCharsets.UTF_8)
                            );
                            if (result.hasError()) {
                                throw new AssertionError(result.error);
                            }
                        }
                    }
                    return messagesPerProducer;
                }));
            }

            Future<Integer> firstConsumer = executor.submit(() ->
                pollUntilMessageCount(port, "stream", 0, expectedMessages)
            );
            Future<Integer> secondConsumer = executor.submit(() ->
                pollUntilMessageCount(port, "stream", 0, expectedMessages)
            );

            startSignal.countDown();

            int produced = 0;
            for (Future<Integer> future : producerFutures) {
                produced += future.get(10, TimeUnit.SECONDS);
            }

            assertEquals(expectedMessages, produced);
            assertEquals(expectedMessages, firstConsumer.get(10, TimeUnit.SECONDS));
            assertEquals(expectedMessages, secondConsumer.get(10, TimeUnit.SECONDS));
            executor.shutdown();
        } finally {
            broker.close();
        }
    }

    @Test
    void testConsumerGroupOffsetCommitAndFetchAcrossClients() throws Exception {
        SimpleKafkaBroker broker = BrokerFactory.create(0, tempDir);
        broker.start();

        try {
            int port = broker.getPort();
            try (SimpleKafkaClient producer = new SimpleKafkaClient("localhost", port)) {
                Protocol.CreateTopicResult createResult = producer.createTopic("audit", 1, (short)1);
                assertFalse(createResult.hasError());

                producer.produce("audit", 0, "audit-1".getBytes(StandardCharsets.UTF_8));
                producer.produce("audit", 0, "audit-2".getBytes(StandardCharsets.UTF_8));
            }

            try (SimpleKafkaClient firstConsumer = new SimpleKafkaClient("localhost", port)) {
                Protocol.FetchResult fetchResult = firstConsumer.fetch("audit", 0, 0, 1024);
                assertFalse(fetchResult.hasError());
                assertEquals(2, fetchResult.messages.size());

                Protocol.OffsetCommitResult commitResult = firstConsumer.commitOffset(
                    "audit-workers",
                    "audit",
                    0,
                    2L
                );
                assertFalse(commitResult.hasError());
            }

            try (SimpleKafkaClient secondConsumer = new SimpleKafkaClient("localhost", port)) {
                Protocol.OffsetFetchResult offsetResult = secondConsumer.fetchCommittedOffset(
                    "audit-workers",
                    "audit",
                    0
                );
                assertFalse(offsetResult.hasError());
                assertEquals(2L, offsetResult.offset);

                Protocol.OffsetFetchResult otherGroup = secondConsumer.fetchCommittedOffset(
                    "unrelated-workers",
                    "audit",
                    0
                );
                assertFalse(otherGroup.hasError());
                assertEquals(-1L, otherGroup.offset);
            }
        } finally {
            broker.close();
        }
    }

    @Test
    void testConsumerInSameGroupResumesFromCommittedOffset() throws Exception {
        SimpleKafkaBroker broker = BrokerFactory.create(0, tempDir);
        broker.start();

        try {
            int port = broker.getPort();
            try (SimpleKafkaClient producer = new SimpleKafkaClient("localhost", port)) {
                Protocol.CreateTopicResult createResult = producer.createTopic("email", 1, (short)1);
                assertFalse(createResult.hasError());

                producer.produce("email", 0, "email-1".getBytes(StandardCharsets.UTF_8));
                producer.produce("email", 0, "email-2".getBytes(StandardCharsets.UTF_8));
                producer.produce("email", 0, "email-3".getBytes(StandardCharsets.UTF_8));
            }

            // First consumer subscribes, polls via group-coordinated path, commits
            try (SimpleKafkaConsumer firstConsumer = new SimpleKafkaConsumer(
                "localhost",
                port,
                "email-workers"
            )) {
                firstConsumer.subscribe("email");
                List<SimpleKafkaConsumer.ConsumerBatch> batches = firstConsumer.poll(1024);
                assertEquals(1, batches.size());
                SimpleKafkaConsumer.ConsumerBatch batch = batches.get(0);
                assertEquals(0L, batch.startOffset());
                assertEquals(3, batch.messages().size());
                firstConsumer.commit(batch);
                assertEquals(3L, firstConsumer.committedOffset("email", 0));
            }

            try (SimpleKafkaClient producer = new SimpleKafkaClient("localhost", port)) {
                producer.produce("email", 0, "email-4".getBytes(StandardCharsets.UTF_8));
            }

            // Second consumer in the same group resumes from committed offset
            try (SimpleKafkaConsumer secondConsumer = new SimpleKafkaConsumer(
                "localhost",
                port,
                "email-workers"
            )) {
                secondConsumer.subscribe("email");
                List<SimpleKafkaConsumer.ConsumerBatch> batches = secondConsumer.poll(1024);
                assertEquals(1, batches.size());
                SimpleKafkaConsumer.ConsumerBatch resumedBatch = batches.get(0);
                assertEquals(3L, resumedBatch.startOffset());
                assertEquals(1, resumedBatch.messages().size());
                assertEquals(
                    "email-4",
                    new String(resumedBatch.messages().get(0), StandardCharsets.UTF_8)
                );
            }
        } finally {
            broker.close();
        }
    }

    private static ByteBuffer send(int port, ByteBuffer request) throws IOException {
        try (Socket socket = new Socket("localhost", port)) {
            Frame.write(socket.getOutputStream(), request);
            return Frame.read(socket.getInputStream());
        }
    }

    private static int fetchMessageCount(int port, String topic, int partition) throws IOException {
        try (SimpleKafkaClient client = new SimpleKafkaClient("localhost", port)) {
            Protocol.FetchResult result = client.fetch(topic, partition, 0, 1024 * 1024);
            if (result.hasError()) {
                throw new AssertionError(result.error);
            }
            return result.messages.size();
        }
    }

    private static int pollUntilMessageCount(
        int port,
        String topic,
        int partition,
        int expectedMessages
    ) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        int latestCount = 0;

        try (SimpleKafkaClient client = new SimpleKafkaClient("localhost", port)) {
            while (System.nanoTime() < deadline) {
                Protocol.FetchResult result = client.fetch(topic, partition, 0, 1024 * 1024);
                if (result.hasError()) {
                    throw new AssertionError(result.error);
                }

                latestCount = result.messages.size();
                if (latestCount == expectedMessages) {
                    return latestCount;
                }
                Thread.sleep(10);
            }
        }

        return latestCount;
    }
}
