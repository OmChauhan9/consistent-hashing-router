package com.distributedsystems.consistenthashing.simulation;

import com.distributedsystems.consistenthashing.model.PhysicalNode;
import com.distributedsystems.consistenthashing.ring.ConsistentHashRing;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Empirical distribution analyzer evaluating key balance variance,
 * standard deviation, and coefficient of variation across physical nodes
 * as a function of virtual node counts (V).
 */
public class DistributionAnalyzer {

    public static class DistributionStats {
        private final int vnodesPerNode;
        private final int totalKeys;
        private final int physicalNodeCount;
        private final long minKeys;
        private final long maxKeys;
        private final double meanKeys;
        private final double standardDeviation;
        private final double coefficientOfVariationPercent; // CV = (stdDev / mean) * 100
        private final double maxMinRatio;
        private final Map<String, Long> keysPerNode;

        public DistributionStats(
                int vnodesPerNode,
                int totalKeys,
                int physicalNodeCount,
                long minKeys,
                long maxKeys,
                double meanKeys,
                double standardDeviation,
                double coefficientOfVariationPercent,
                double maxMinRatio,
                Map<String, Long> keysPerNode) {
            this.vnodesPerNode = vnodesPerNode;
            this.totalKeys = totalKeys;
            this.physicalNodeCount = physicalNodeCount;
            this.minKeys = minKeys;
            this.maxKeys = maxKeys;
            this.meanKeys = meanKeys;
            this.standardDeviation = standardDeviation;
            this.coefficientOfVariationPercent = coefficientOfVariationPercent;
            this.maxMinRatio = maxMinRatio;
            this.keysPerNode = keysPerNode != null ? Collections.unmodifiableMap(keysPerNode) : Collections.emptyMap();
        }

        public int getVnodesPerNode() {
            return vnodesPerNode;
        }

        public int getTotalKeys() {
            return totalKeys;
        }

        public int getPhysicalNodeCount() {
            return physicalNodeCount;
        }

        public long getMinKeys() {
            return minKeys;
        }

        public long getMaxKeys() {
            return maxKeys;
        }

        public double getMeanKeys() {
            return meanKeys;
        }

        public double getStandardDeviation() {
            return standardDeviation;
        }

        public double getCoefficientOfVariationPercent() {
            return coefficientOfVariationPercent;
        }

        public double getMaxMinRatio() {
            return maxMinRatio;
        }

        public Map<String, Long> getKeysPerNode() {
            return keysPerNode;
        }

        /**
         * Renders an ASCII horizontal bar chart representing key share per node.
         */
        public String renderAsciiChart() {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("Distribution (V=%d, Keys=%d, Mean=%.1f, StdDev=%.2f, CV=%.2f%%):\n",
                    vnodesPerNode, totalKeys, meanKeys, standardDeviation, coefficientOfVariationPercent));

            long max = Math.max(1, maxKeys);
            int barWidth = 30;

            for (Map.Entry<String, Long> entry : keysPerNode.entrySet()) {
                long count = entry.getValue();
                double pct = (count * 100.0) / totalKeys;
                int filled = (int) Math.round(((double) count / max) * barWidth);
                String bar = "█".repeat(filled) + "░".repeat(Math.max(0, barWidth - filled));
                sb.append(String.format("  %-12s [%s] %6d (%5.2f%%)\n", entry.getKey(), bar, count, pct));
            }
            return sb.toString();
        }

        @Override
        public String toString() {
            return String.format("V=%-4d | Mean=%-8.1f | Min=%-6d | Max=%-6d | StdDev=%-7.2f | CV=%-6.2f%% | Max/Min=%-4.2fx",
                    vnodesPerNode, meanKeys, minKeys, maxKeys, standardDeviation, coefficientOfVariationPercent, maxMinRatio);
        }
    }

    /**
     * Hashes sample keys onto the ring and analyzes key balance distribution.
     *
     * @param ring consistent hash ring populated with physical nodes
     * @param vnodesPerNode virtual nodes per physical machine
     * @param sampleKeys list of string keys to route
     * @return statistical distribution metrics
     */
    public static DistributionStats analyze(ConsistentHashRing ring, int vnodesPerNode, List<String> sampleKeys) {
        if (ring == null || sampleKeys == null || sampleKeys.isEmpty()) {
            throw new IllegalArgumentException("Ring and sampleKeys must be valid and non-empty");
        }

        List<PhysicalNode> physicalNodes = ring.getAllPhysicalNodes();
        if (physicalNodes.isEmpty()) {
            throw new IllegalStateException("Ring has no physical nodes registered");
        }

        Map<String, Long> keyCounts = new LinkedHashMap<>();
        for (PhysicalNode node : physicalNodes) {
            keyCounts.put(node.getId(), 0L);
        }

        // Route each key to its primary coordinator
        for (String key : sampleKeys) {
            PhysicalNode primary = ring.getPrimaryNode(key);
            if (primary != null) {
                keyCounts.merge(primary.getId(), 1L, Long::sum);
            }
        }

        int n = physicalNodes.size();
        int total = sampleKeys.size();
        double mean = (double) total / n;

        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        double sumSquaredDiff = 0.0;

        for (long count : keyCounts.values()) {
            if (count < min) min = count;
            if (count > max) max = count;
            double diff = count - mean;
            sumSquaredDiff += diff * diff;
        }

        double stdDev = Math.sqrt(sumSquaredDiff / n);
        double cv = (stdDev / mean) * 100.0;
        double maxMinRatio = min > 0 ? (double) max / min : 0.0;

        return new DistributionStats(
                vnodesPerNode,
                total,
                n,
                min,
                max,
                mean,
                stdDev,
                cv,
                maxMinRatio,
                keyCounts
        );
    }
}
