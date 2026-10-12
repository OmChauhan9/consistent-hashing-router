package com.distributedsystems.consistenthashing.quorum;

/**
 * Thrown when a read or write operation fails to achieve the requested quorum.
 */
public class QuorumException extends RuntimeException {

    private final String key;
    private final int requiredQuorum;
    private final int actualAcks;

    public QuorumException(String key, int requiredQuorum, int actualAcks, String message) {
        super(String.format("Key '%s': Required quorum of %d failed (only %d succeeded). %s",
                key, requiredQuorum, actualAcks, message));
        this.key = key;
        this.requiredQuorum = requiredQuorum;
        this.actualAcks = actualAcks;
    }

    public QuorumException(String key, int requiredQuorum, int actualAcks, String message, Throwable cause) {
        super(String.format("Key '%s': Required quorum of %d failed (only %d succeeded). %s",
                key, requiredQuorum, actualAcks, message), cause);
        this.key = key;
        this.requiredQuorum = requiredQuorum;
        this.actualAcks = actualAcks;
    }

    public String getKey() {
        return key;
    }

    public int getRequiredQuorum() {
        return requiredQuorum;
    }

    public int getActualAcks() {
        return actualAcks;
    }
}
