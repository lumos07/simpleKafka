package com.simplekafka.broker;

import com.simplekafka.protocol.Protocol;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import com.simplekafka.client.SimpleKafkaClient;

public class ReadBenchmark {
    public static void main(String[] args) throws Exception {
        System.setProperty("kafka.flush.messages", "1000");
        System.setProperty("kafka.flush.ms", "100");

        Path tempDir = Files.createTempDirectory("kafka-read-bench");
        SimpleKafkaBroker broker = BrokerFactory.create(0, tempDir);
        broker.start();
        int port = broker.getPort();

        int totalMessages = 1_000_000;

        try (SimpleKafkaClient client = new SimpleKafkaClient("localhost", port)) {
            client.createTopic("perf-read", 1, (short) 1);
            System.out.println("Producing " + totalMessages + " messages (1GB total)...");
            byte[] msg = new byte[1024];
            for (int i = 0; i < totalMessages; i++) {
                client.produce("perf-read", 0, msg);
            }
            // Give it a moment to flush everything
            Thread.sleep(500);

            System.out.println("\nTesting Read Latency (O(N) behavior):");
            
            // Warmup reads to get JVM hot
            for (int i = 0; i < 100; i++) {
                client.fetch("perf-read", 0, 0, 1024);
            }

            long[] targetOffsets = {0, 250_000, 500_000, 750_000, 999_999};

            System.out.println("Warming up JIT compiler...");
            for (int j = 0; j < 5; j++) {
                for (long offset : targetOffsets) {
                    client.fetch("perf-read", 0, offset, 1024);
                }
            }

            System.out.println("| Target Offset | Read Latency (ms) | Fetched Count |");
            System.out.println("|---------------|-------------------|---------------|");

            for (long offset : targetOffsets) {
                long start = System.nanoTime();
                int iterations = 10;
                int fetchedCount = 0;
                for (int j = 0; j < iterations; j++) {
                    Protocol.FetchResult res = client.fetch("perf-read", 0, offset, 1024);
                    if (res.hasError()) {
                        System.err.println("Fetch error for offset " + offset + ": " + res.error);
                        break;
                    }
                    fetchedCount = res.messages.size();
                }
                long end = System.nanoTime();
                
                double durationMs = (end - start) / 1_000_000.0 / iterations;
                System.out.printf("| %13d | %17.4f | %13d |\n", offset, durationMs, fetchedCount);
            }

        } finally {
            broker.close();
        }
    }
}
