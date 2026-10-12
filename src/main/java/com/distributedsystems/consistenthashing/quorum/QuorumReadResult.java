package com.distributedsystems.consistenthashing.quorum;

import com.distributedsystems.consistenthashing.model.PhysicalNode;
import com.distributedsystems.consistenthashing.model.VersionedValue;
import com.distributedsystems.consistenthashing.ring.PreferenceList;

import java.util.Collections;
import java.util.List;

/**
 * Encapsulates the execution result and metadata of a distributed quorum read,
 * including resolved versioned payload and read repair telemetry.
 */
public class QuorumReadResult {

    private final String key;
    private final VersionedValue value;
    private final boolean found;
    private final int responsesReceived;
    private final int readQuorumRequired;
    private final PreferenceList preferenceList;
    private final boolean readRepairTriggered;
    private final List<PhysicalNode> repairedNodes;
    private final long latencyNanos;

    public QuorumReadResult(
            String key,
            VersionedValue value,
            boolean found,
            int responsesReceived,
            int readQuorumRequired,
            PreferenceList preferenceList,
            boolean readRepairTriggered,
            List<PhysicalNode> repairedNodes,
            long latencyNanos) {
        this.key = key;
        this.value = value;
        this.found = found;
        this.responsesReceived = responsesReceived;
        this.readQuorumRequired = readQuorumRequired;
        this.preferenceList = preferenceList;
        this.readRepairTriggered = readRepairTriggered;
        this.repairedNodes = repairedNodes != null ? List.copyOf(repairedNodes) : Collections.emptyList();
        this.latencyNanos = latencyNanos;
    }

    public String getKey() {
        return key;
    }

    public VersionedValue getValue() {
        return value;
    }

    public String getValueAsString() {
        return value != null ? value.getValueAsString() : null;
    }

    public boolean isFound() {
        return found;
    }

    public int getResponsesReceived() {
        return responsesReceived;
    }

    public int getReadQuorumRequired() {
        return readQuorumRequired;
    }

    public PreferenceList getPreferenceList() {
        return preferenceList;
    }

    public boolean isReadRepairTriggered() {
        return readRepairTriggered;
    }

    public List<PhysicalNode> getRepairedNodes() {
        return repairedNodes;
    }

    public long getLatencyNanos() {
        return latencyNanos;
    }

    public double getLatencyMicros() {
        return latencyNanos / 1000.0;
    }

    @Override
    public String toString() {
        return "QuorumReadResult{" +
                "key='" + key + '\'' +
                ", found=" + found +
                ", val='" + (value != null ? value.getValueAsString() : "null") + '\'' +
                ", responses=" + responsesReceived + "/" + readQuorumRequired +
                ", repaired=" + readRepairTriggered +
                ", latencyMicros=" + getLatencyMicros() +
                '}';
    }
}
