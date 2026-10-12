package com.distributedsystems.consistenthashing.storage;

/**
 * Thrown when an operation is dispatched to a node that is crashed, unreachable, or timed out.
 */
public class NodeUnavailableException extends RuntimeException {

    public NodeUnavailableException(String message) {
        super(message);
    }

    public NodeUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
