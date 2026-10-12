package com.distributedsystems.consistenthashing.simulation;

import com.distributedsystems.consistenthashing.hash.HashFunction;
import com.distributedsystems.consistenthashing.hash.Murmur3Hash;
import com.distributedsystems.consistenthashing.model.PhysicalNode;
import com.distributedsystems.consistenthashing.ring.ConsistentHashRing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Node Churn simulator comparing key redistribution mechanics under
 * Consistent Hashing vs Naive Modulo Hashing during cluster scale-out and node failure.
 *
 * <p>Empirically proves that Consistent Hashing moves only ~K/(N+1) keys upon node addition,
 * whereas Modulo Hashing relocates ~N/(N+1) (>90%) of all keys across the cluster.</p>
 */
public class NodeChurnSimulator {

    public static class ChurnComparisonResult {
        private final int initialNodes;
        private final int finalNodes;
        private final int totalKeys;
        private final int consistentHashingKeysMoved;
        private final double consistentHashingPercentMoved;
        private final int moduloHashingKeysMoved;
        private final double moduloHashingPercentMoved;
        private final double theoreticalOptimalPercent;

        public ChurnComparisonResult(
                int initialNodes,
                int finalNodes,
                int totalKeys,
                int consistentHashingKeysMoved,
                int moduloHashingKeysMoved,
                double theoreticalOptimalPercent) {
            this.initialNodes = initialNodes;
            this.finalNodes = finalNodes;
            this.totalKeys = totalKeys;
            this.consistentHashingKeysMoved = consistentHashingKeysMoved;
            this.consistentHashingPercentMoved = (consistentHashingKeysMoved * 100.0) / totalKeys;
            this.moduloHashingKeysMoved = moduloHashingKeysMoved;
            this.moduloHashingPercentMoved = (moduloHashingKeysMoved * 100.0) / totalKeys;
            this.theoreticalOptimalPercent = theoreticalOptimalPercent;
        }

        public int getInitialNodes() {
            return initialNodes;
        }

        public int getFinalNodes() {
            return finalNodes;
        }

        public int getTotalKeys() {
            return totalKeys;
        }

        public int getConsistentHashingKeysMoved() {
            return consistentHashingKeysMoved;
        }

        public double getConsistentHashingPercentMoved() {
            return consistentHashingPercentMoved;
        }

        public int getModuloHashingKeysMoved() {
            return moduloHashingKeysMoved;
        }

        public double getModuloHashingPercentMoved() {
            return moduloHashingPercentMoved;
        }

        public double getTheoreticalOptimalPercent() {
            return theoreticalOptimalPercent;
        }

        @Override
        public String toString() {
            return String.format(
                    "Churn Simulation (%d -> %d nodes, %d keys):\n" +
                    "  [Theoretical Min]: %.2f%% keys moved\n" +
                    "  [Consistent Hash]: %d keys moved (%.2f%%) [Optimal ~K/N bounded]\n" +
                    "  [Modulo Hashing ]: %d keys moved (%.2f%%) [Data Migration Storm!]",
                    initialNodes, finalNodes, totalKeys,
                    theoreticalOptimalPercent,
                    consistentHashingKeysMoved, consistentHashingPercentMoved,
                    moduloHashingKeysMoved, moduloHashingPercentMoved);
        }
    }

    /**
     * Simulates adding a node to a cluster (e.g. from N to N+1 nodes)
     * and compares key movement between Consistent Hashing and Modulo Hashing.
     */
    public static ChurnComparisonResult simulateNodeAddition(
            int initialNodeCount,
            int vnodesPerNode,
            List<String> keys) {

        HashFunction hashFunc = new Murmur3Hash();
        ConsistentHashRing ring = new ConsistentHashRing(hashFunc, vnodesPerNode);

        List<PhysicalNode> initialNodes = new ArrayList<>();
        for (int i = 0; i < initialNodeCount; i++) {
            PhysicalNode node = new PhysicalNode("node-" + i);
            initialNodes.add(node);
            ring.addPhysicalNode(node, vnodesPerNode);
        }

        // Record baseline locations under initial N nodes
        Map<String, String> consistentBefore = new HashMap<>();
        Map<String, Integer> moduloBefore = new HashMap<>();

        for (String key : keys) {
            consistentBefore.put(key, ring.getPrimaryNode(key).getId());
            long hash = hashFunc.hash(key);
            int modIndex = (int) (hash % initialNodeCount);
            moduloBefore.put(key, modIndex);
        }

        // Add (N+1)-th node
        PhysicalNode newNode = new PhysicalNode("node-" + initialNodeCount);
        ring.addPhysicalNode(newNode, vnodesPerNode);
        int finalNodeCount = initialNodeCount + 1;

        // Measure relocations
        int consistentMoved = 0;
        int moduloMoved = 0;

        for (String key : keys) {
            String consistentAfter = ring.getPrimaryNode(key).getId();
            if (!consistentAfter.equals(consistentBefore.get(key))) {
                consistentMoved++;
            }

            long hash = hashFunc.hash(key);
            int modAfter = (int) (hash % finalNodeCount);
            if (modAfter != moduloBefore.get(key)) {
                moduloMoved++;
            }
        }

        double theoreticalOptimal = (1.0 / finalNodeCount) * 100.0;

        return new ChurnComparisonResult(
                initialNodeCount,
                finalNodeCount,
                keys.size(),
                consistentMoved,
                moduloMoved,
                theoreticalOptimal
        );
    }

    /**
     * Simulates removing a node from a cluster (e.g. from N to N-1 nodes)
     * and compares key movement between Consistent Hashing and Modulo Hashing.
     */
    public static ChurnComparisonResult simulateNodeRemoval(
            int initialNodeCount,
            int vnodesPerNode,
            List<String> keys) {

        HashFunction hashFunc = new Murmur3Hash();
        ConsistentHashRing ring = new ConsistentHashRing(hashFunc, vnodesPerNode);

        List<PhysicalNode> initialNodes = new ArrayList<>();
        for (int i = 0; i < initialNodeCount; i++) {
            PhysicalNode node = new PhysicalNode("node-" + i);
            initialNodes.add(node);
            ring.addPhysicalNode(node, vnodesPerNode);
        }

        // Record baseline locations under initial N nodes
        Map<String, String> consistentBefore = new HashMap<>();
        Map<String, Integer> moduloBefore = new HashMap<>();

        for (String key : keys) {
            consistentBefore.put(key, ring.getPrimaryNode(key).getId());
            long hash = hashFunc.hash(key);
            int modIndex = (int) (hash % initialNodeCount);
            moduloBefore.put(key, modIndex);
        }

        // Remove the last node
        PhysicalNode removedNode = initialNodes.get(initialNodeCount - 1);
        ring.removePhysicalNode(removedNode);
        int finalNodeCount = initialNodeCount - 1;

        int consistentMoved = 0;
        int moduloMoved = 0;

        for (String key : keys) {
            String consistentAfter = ring.getPrimaryNode(key).getId();
            if (!consistentAfter.equals(consistentBefore.get(key))) {
                consistentMoved++;
            }

            long hash = hashFunc.hash(key);
            int modAfter = (int) (hash % finalNodeCount);
            if (modAfter != moduloBefore.get(key)) {
                moduloMoved++;
            }
        }

        double theoreticalOptimal = (1.0 / initialNodeCount) * 100.0;

        return new ChurnComparisonResult(
                initialNodeCount,
                finalNodeCount,
                keys.size(),
                consistentMoved,
                moduloMoved,
                theoreticalOptimal
        );
    }
}
