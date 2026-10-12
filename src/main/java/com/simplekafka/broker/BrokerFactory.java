package com.simplekafka.broker;

import com.simplekafka.common.ConsistentHashAssignmentStrategy;
import com.simplekafka.common.PartitionAssignmentStrategy;
import com.simplekafka.storage.TopicMetadata;

import java.nio.file.Path;

/**
 * Factory that assembles a fully-wired SimpleKafkaBroker.
 *
 * This is the only place in the codebase that knows about concrete
 * implementations — isolating wiring decisions from domain logic.
 */
public class BrokerFactory {

    private static final Path DEFAULT_DATA_DIR = Path.of("data");

    /**
     * Creates a broker with default configuration:
     * - File-backed storage in "data/"
     * - ConsistentHash partition assignment
     */
    public static SimpleKafkaBroker create(int port) {
        return create(port, DEFAULT_DATA_DIR);
    }

    public static SimpleKafkaBroker create(int port, Path dataDir) {
        return create(port, new BrokerState(dataDir), new ConsistentHashAssignmentStrategy());
    }

    public static SimpleKafkaBroker create(int port, BrokerState state, PartitionAssignmentStrategy strategy) {
        ConsumerGroupCoordinator coordinator = new ConsumerGroupCoordinator(topic -> {
            if (!state.hasTopic(topic)) {
                throw new IllegalArgumentException("Unknown topic: " + topic);
            }
            for (TopicMetadata md : state.metadata()) {
                if (md.name().equals(topic)) {
                    return md.partitionCount();
                }
            }
            throw new IllegalArgumentException("Unknown topic: " + topic);
        }, strategy);

        ProducerStateManager producerState = new ProducerStateManager();

        return new SimpleKafkaBroker(port, state, coordinator, producerState);
    }
}
