package com.simplekafka.common;

/**
 * A domain-level representation of a topic-partition pair.
 * Shared across the client and broker packages without
 * coupling either to the wire-format Protocol class.
 */
public record TopicPartition(String topic, int partition) {}
