package com.simplekafka.broker;

import java.nio.file.Files;
import java.nio.file.Path;

import com.simplekafka.client.SimpleKafkaClient;

public class Benchmark {
    public static void main(String[] args) throws Exception {
        int[][] configs = {
            {10, 10},
            {100, 50},
            {500, 100},
            {1000, 100},
            {5000, 250},
            {10000, 500}
        };

        System.out.println("| Messages Limit | Time Limit (ms) | Throughput (msgs/sec) | Avg Latency (ms) |");
        System.out.println("|----------------|-----------------|-----------------------|------------------|");

        for (int[] config : configs) {
            int flushMessages = config[0];
            int flushMs = config[1];

            System.setProperty("kafka.flush.messages", String.valueOf(flushMessages));
            System.setProperty("kafka.flush.ms", String.valueOf(flushMs));

            Path tempDir = Files.createTempDirectory("kafka-bench-" + flushMessages);
            SimpleKafkaBroker broker = BrokerFactory.create(0, tempDir);
            broker.start();
            int port = broker.getPort();

            try (SimpleKafkaClient client = new SimpleKafkaClient("localhost", port)) {
                byte[] msg = new byte[64];
                // Warmup
                for (int i = 0; i < 1000; i++) {
                    client.produce("perf", 0, msg);
                }

                long start = System.nanoTime();
                int count = 10000;
                for (int i = 0; i < count; i++) {
                    client.produce("perf", 0, msg);
                }
                long end = System.nanoTime();
                
                double durationMs = (end - start) / 1_000_000.0;
                double msgsPerSec = count / (durationMs / 1000.0);
                double avgLatency = durationMs / count;
                
                System.out.printf("| %14d | %15d | %21.2f | %16.4f |\n", flushMessages, flushMs, msgsPerSec, avgLatency);
            } finally {
                broker.close();
            }
        }
    }
}
