package com.distributedsystems.consistenthashing.quorum;

import com.distributedsystems.consistenthashing.model.PhysicalNode;
import com.distributedsystems.consistenthashing.ring.PreferenceList;

import java.util.Collections;
import java.util.List;

/**
 * Encapsulates the execution result and metadata of a distributed quorum write.
 */
public class QuorumWriteResult {

    private final String key;
    private final boolean success;
    private final int acksReceived;
    private final int writeQuorumRequired;
    private final PreferenceList preferenceList;
    private final List<PhysicalNode> acknowledgedNodes;
    private final List<PhysicalNode> failedNodes;
    private final long latencyNanos;

    public QuorumWriteResult(
            String key,
            boolean success,
            int acksReceived,
            int writeQuorumRequired,
            PreferenceList preferenceList,
            List<PhysicalNode> acknowledgedNodes,
            List<PhysicalNode> failedNodes,
            long latencyNanos) {
        this.key = key;
        this.success = success;
        this.acksReceived = acksReceived;
        this.writeQuorumRequired = writeQuorumRequired;
        this.preferenceList = preferenceList;
        this.acknowledgedNodes = acknowledgedNodes != null ? List.copyOf(acknowledgedNodes) : Collections.emptyList();
        this.failedNodes = failedNodes != null ? List.copyOf(failedNodes) : Collections.emptyList();
        this.latencyNanos = latencyNanos;
    }

    public String getKey() {
        return key;
    }

    public boolean isSuccess() {
        return success;
    }

    public int getAcksReceived() {
        return acksReceived;
    }

    public int getWriteQuorumRequired() {
        return writeQuorumRequired;
    }

    public PreferenceList getPreferenceList() {
        return preferenceList;
    }

    public List<PhysicalNode> getAcknowledgedNodes() {
        return acknowledgedNodes;
    }

    public List<PhysicalNode> getFailedNodes() {
        return failedNodes;
    }

    public long getLatencyNanos() {
        return latencyNanos;
    }

    public double getLatencyMicros() {
        return latencyNanos / 1000.0;
    }

    @Override
    public String toString() {
        return "QuorumWriteResult{" +
                "key='" + key + '\'' +
                ", success=" + success +
                ", acks=" + acksReceived + "/" + writeQuorumRequired +
                ", latencyMicros=" + getLatencyMicros() +
                ", ackedNodes=" + acknowledgedNodes +
                '}';
    }
}
