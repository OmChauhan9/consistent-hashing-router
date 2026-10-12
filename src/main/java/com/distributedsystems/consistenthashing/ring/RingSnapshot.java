package com.distributedsystems.consistenthashing.ring;

import com.distributedsystems.consistenthashing.model.PhysicalNode;

import java.util.Collections;
import java.util.Map;

/**
 * Immutable snapshot of the consistent hash ring state at a given instant,
 * including token counts, arc coverage percentages, and physical membership.
 */
public class RingSnapshot {

    private final int physicalNodeCount;
    private final int totalVirtualNodes;
    private final Map<PhysicalNode, Integer> vnodeCountPerNode;
    private final Map<PhysicalNode, Double> arcCoveragePercentPerNode;
    private final long ringSpaceSize;

    public RingSnapshot(
            int physicalNodeCount,
            int totalVirtualNodes,
            Map<PhysicalNode, Integer> vnodeCountPerNode,
            Map<PhysicalNode, Double> arcCoveragePercentPerNode,
            long ringSpaceSize) {
        this.physicalNodeCount = physicalNodeCount;
        this.totalVirtualNodes = totalVirtualNodes;
        this.vnodeCountPerNode = vnodeCountPerNode != null ? Map.copyOf(vnodeCountPerNode) : Collections.emptyMap();
        this.arcCoveragePercentPerNode = arcCoveragePercentPerNode != null ? Map.copyOf(arcCoveragePercentPerNode) : Collections.emptyMap();
        this.ringSpaceSize = ringSpaceSize;
    }

    public int getPhysicalNodeCount() {
        return physicalNodeCount;
    }

    public int getTotalVirtualNodes() {
        return totalVirtualNodes;
    }

    public Map<PhysicalNode, Integer> getVnodeCountPerNode() {
        return vnodeCountPerNode;
    }

    public Map<PhysicalNode, Double> getArcCoveragePercentPerNode() {
        return arcCoveragePercentPerNode;
    }

    public long getRingSpaceSize() {
        return ringSpaceSize;
    }

    @Override
    public String toString() {
        return "RingSnapshot{" +
                "nodes=" + physicalNodeCount +
                ", vnodes=" + totalVirtualNodes +
                ", arcCoverage=" + arcCoveragePercentPerNode +
                '}';
    }
}
