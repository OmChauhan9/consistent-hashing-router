package com.distributedsystems.consistenthashing;

import com.distributedsystems.consistenthashing.simulation.BenchmarkRunner;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RoutingLatencyBenchmarkTest {

    @Test
    @DisplayName("Hash Ring routing resolution introduces < 5 microseconds average latency per request")
    void shouldMaintainSubFiveMicrosecondRoutingLatency() {
        int nodeCount = 10;
        int vnodes = 100;
        int operations = 50_000;
        int replicationFactor = 3;

        BenchmarkRunner.BenchmarkResult result =
                BenchmarkRunner.runPreferenceListBenchmark(nodeCount, vnodes, operations, replicationFactor);

        System.out.println("Microbenchmark Test Result:\n" + result);

        // Core requirement verification: Average route lookup latency must be < 5.0 microseconds
        assertThat(result.getAvgLatencyMicros())
                .isLessThan(5.0);

        // Throughput must exceed 200,000 ops/sec
        assertThat(result.getThroughputOpsPerSec())
                .isGreaterThan(200_000);
    }
}
