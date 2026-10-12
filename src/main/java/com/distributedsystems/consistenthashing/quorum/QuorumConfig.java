package com.distributedsystems.consistenthashing.quorum;

/**
 * Tunable Quorum parameters governing replication, consistency guarantees,
 * and fault tolerance thresholds.
 */
public class QuorumConfig {

    private final int replicationFactor; // N
    private final int writeQuorum;       // W
    private final int readQuorum;        // R
    private final long timeoutMillis;
    private final boolean readRepairEnabled;
    private final boolean sloppyQuorumEnabled;

    public static QuorumConfig strict(int n) {
        int w = (n / 2) + 1;
        int r = (n / 2) + 1;
        return new QuorumConfig(n, w, r, 2000, true, false);
    }

    public static QuorumConfig defaultDynamo() {
        return new QuorumConfig(3, 2, 2, 2000, true, true);
    }

    public QuorumConfig(int replicationFactor, int writeQuorum, int readQuorum) {
        this(replicationFactor, writeQuorum, readQuorum, 2000, true, true);
    }

    public QuorumConfig(
            int replicationFactor,
            int writeQuorum,
            int readQuorum,
            long timeoutMillis,
            boolean readRepairEnabled,
            boolean sloppyQuorumEnabled) {
        if (replicationFactor <= 0) {
            throw new IllegalArgumentException("Replication factor (N) must be > 0");
        }
        if (writeQuorum <= 0 || writeQuorum > replicationFactor) {
            throw new IllegalArgumentException("Write quorum (W) must satisfy 1 <= W <= N");
        }
        if (readQuorum <= 0 || readQuorum > replicationFactor) {
            throw new IllegalArgumentException("Read quorum (R) must satisfy 1 <= R <= N");
        }
        if (timeoutMillis <= 0) {
            throw new IllegalArgumentException("Timeout must be > 0");
        }

        this.replicationFactor = replicationFactor;
        this.writeQuorum = writeQuorum;
        this.readQuorum = readQuorum;
        this.timeoutMillis = timeoutMillis;
        this.readRepairEnabled = readRepairEnabled;
        this.sloppyQuorumEnabled = sloppyQuorumEnabled;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public int getWriteQuorum() {
        return writeQuorum;
    }

    public int getReadQuorum() {
        return readQuorum;
    }

    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    public boolean isReadRepairEnabled() {
        return readRepairEnabled;
    }

    public boolean isSloppyQuorumEnabled() {
        return sloppyQuorumEnabled;
    }

    /**
     * Determines whether the quorum configuration satisfies strong consistency:
     * W + R > N guarantees that the write quorum and read quorum intersect on
     * at least one replica holding the latest version.
     */
    public boolean isStronglyConsistent() {
        return (writeQuorum + readQuorum) > replicationFactor;
    }

    @Override
    public String toString() {
        return "QuorumConfig{" +
                "N=" + replicationFactor +
                ", W=" + writeQuorum +
                ", R=" + readQuorum +
                ", strongConsistency=" + isStronglyConsistent() +
                ", readRepair=" + readRepairEnabled +
                ", sloppyQuorum=" + sloppyQuorumEnabled +
                '}';
    }
}
