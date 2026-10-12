package com.simplekafka.broker;

/**
 * Thrown when a producer sends a sequence number that is more than 1 ahead
 * of the last accepted sequence — indicating messages were lost in transit.
 */
class OutOfOrderSequenceException extends IllegalStateException {
    OutOfOrderSequenceException(String message) {
        super(message);
    }
}
