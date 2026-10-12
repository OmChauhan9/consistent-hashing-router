package com.distributedsystems.consistenthashing.simulation;

import com.distributedsystems.consistenthashing.hash.Murmur3Hash;
import com.distributedsystems.consistenthashing.model.PhysicalNode;
import com.distributedsystems.consistenthashing.ring.ConsistentHashRing;
import com.distributedsystems.consistenthashing.ring.PreferenceList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * High-precision microbenchmark quantifying route-resolution latency and throughput
 * under consistent hash ring operations.
 *
 * <p>Validates that ring routing introduces &lt; 5 microseconds overhead per request.</p>
 */
public class BenchmarkRunner {

    public static class BenchmarkResult {
        private final int totalOperations;
        private final int physicalNodes;
        private final int vnodesPerNode;
        private final double totalTimeMillis;
        private final double throughputOpsPerSec;
        private final double avgLatencyMicros;
        private final double p50Micros;
        private final double p90Micros;
        private final double p99Micros;
        private final double p999Micros;

        public BenchmarkResult(
                int totalOperations,
                int physicalNodes,
                int vnodesPerNode,
                double totalTimeMillis,
                double throughputOpsPerSec,
                double avgLatencyMicros,
                double p50Micros,
                double p90Micros,
                double p99Micros,
                double p999Micros) {
            this.totalOperations = totalOperations;
            this.physicalNodes = physicalNodes;
            this.vnodesPerNode = vnodesPerNode;
            this.totalTimeMillis = totalTimeMillis;
            this.throughputOpsPerSec = throughputOpsPerSec;
            this.avgLatencyMicros = avgLatencyMicros;
            this.p50Micros = p50Micros;
            this.p90Micros = p90Micros;
            this.p99Micros = p99Micros;
            this.p999Micros = p999Micros;
        }

        public double getAvgLatencyMicros() {
            return avgLatencyMicros;
        }

        public double getThroughputOpsPerSec() {
            return throughputOpsPerSec;
        }

        public double getP99Micros() {
            return p99Micros;
        }

        @Override
        public String toString() {
            return String.format(
                    "Benchmark Results (%d nodes, %d vnodes, %d ops):\n" +
                    "  Throughput : %,.0f ops/sec\n" +
                    "  Avg Latency: %.3f µs\n" +
                    "  P50 Latency: %.3f µs\n" +
                    "  P90 Latency: %.3f µs\n" +
                    "  P99 Latency: %.3f µs\n" +
                    "  P99.9      : %.3f µs",
                    physicalNodes, vnodesPerNode, totalOperations,
                    throughputOpsPerSec, avgLatencyMicros, p50Micros, p90Micros, p99Micros, p999Micros);
        }
    }

    public static BenchmarkResult runPreferenceListBenchmark(int nodeCount, int vnodes, int iterations, int replicationFactor) {
        ConsistentHashRing ring = new ConsistentHashRing(new Murmur3Hash(), vnodes);
        for (int i = 0; i < nodeCount; i++) {
            ring.addPhysicalNode(new PhysicalNode("server-" + i), vnodes);
        }

        List<String> testKeys = new ArrayList<>(iterations);
        for (int i = 0; i < iterations; i++) {
            testKeys.add("user_profile_session_uuid_" + i);
        }

        // JVM Warmup phase (JIT compile hot paths)
        int warmup = Math.min(20000, iterations / 2);
        for (int i = 0; i < warmup; i++) {
            ring.getPreferenceList(testKeys.get(i % testKeys.size()), replicationFactor);
        }

        // Timed measurement phase
        long[] latenciesNanos = new long[iterations];
        long overallStart = System.nanoTime();

        for (int i = 0; i < iterations; i++) {
            long opStart = System.nanoTime();
            PreferenceList list = ring.getPreferenceList(testKeys.get(i), replicationFactor);
            latenciesNanos[i] = System.nanoTime() - opStart;
        }

        long overallEnd = System.nanoTime();
        double totalTimeMillis = (overallEnd - overallStart) / 1_000_000.0;
        double throughput = (iterations / (totalTimeMillis / 1000.0));

        Arrays.sort(latenciesNanos);

        double sumNanos = 0;
        for (long l : latenciesNanos) {
            sumNanos += l;
        }

        double avgMicros = (sumNanos / iterations) / 1000.0;
        double p50Micros = latenciesNanos[(int) (iterations * 0.50)] / 1000.0;
        double p90Micros = latenciesNanos[(int) (iterations * 0.90)] / 1000.0;
        double p99Micros = latenciesNanos[(int) (iterations * 0.99)] / 1000.0;
        double p999Micros = latenciesNanos[(int) (iterations * 0.999)] / 1000.0;

        return new BenchmarkResult(
                iterations,
                nodeCount,
                vnodes,
                totalTimeMillis,
                throughput,
                avgMicros,
                p50Micros,
                p90Micros,
                p99Micros,
                p999Micros
        );
    }
}
