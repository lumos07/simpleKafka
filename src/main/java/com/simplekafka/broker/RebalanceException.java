package com.simplekafka.broker;

/**
 * Thrown when a consumer's generation is stale and a rebalance is required.
 * The consumer must rejoin the group to get fresh partition assignments.
 */
class RebalanceException extends IllegalStateException {
    RebalanceException(String message) {
        super(message);
    }
}
