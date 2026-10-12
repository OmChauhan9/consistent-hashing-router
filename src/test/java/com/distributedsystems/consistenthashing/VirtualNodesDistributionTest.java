package com.distributedsystems.consistenthashing;

import com.distributedsystems.consistenthashing.hash.Murmur3Hash;
import com.distributedsystems.consistenthashing.model.PhysicalNode;
import com.distributedsystems.consistenthashing.ring.ConsistentHashRing;
import com.distributedsystems.consistenthashing.simulation.DistributionAnalyzer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class VirtualNodesDistributionTest {

    @Test
    @DisplayName("Increasing virtual nodes decays distribution variance and standard deviation")
    void shouldDecayVarianceAsVnodesIncrease() {
        int keyCount = 50_000;
        List<String> keys = new ArrayList<>(keyCount);
        for (int i = 0; i < keyCount; i++) {
            keys.add("key_simulation_" + i);
        }

        int nodeCount = 5;
        int[] vnodeLevels = {1, 10, 50, 100, 250};
        double[] cvLevels = new double[vnodeLevels.length];

        for (int idx = 0; idx < vnodeLevels.length; idx++) {
            int v = vnodeLevels[idx];
            ConsistentHashRing ring = new ConsistentHashRing(new Murmur3Hash(), v);
            for (int i = 1; i <= nodeCount; i++) {
                ring.addPhysicalNode(new PhysicalNode("srv-" + i), v);
            }

            DistributionAnalyzer.DistributionStats stats = DistributionAnalyzer.analyze(ring, v, keys);
            cvLevels[idx] = stats.getCoefficientOfVariationPercent();
        }

        // Assert that V=1 exhibits large imbalance (CV > 25%)
        assertThat(cvLevels[0]).isGreaterThan(25.0);

        // Assert that V=100 achieves uniform balance (CV < 15%)
        assertThat(cvLevels[3]).isLessThan(15.0);

        // Assert that V=250 achieves high uniformity (CV < 10%)
        assertThat(cvLevels[4]).isLessThan(10.0);

        // Assert that increasing V significantly reduces variance compared to V=1
        assertThat(cvLevels[0]).isGreaterThan(cvLevels[1]);
        assertThat(cvLevels[1]).isGreaterThan(cvLevels[3]);
        assertThat(cvLevels[0]).isGreaterThan(cvLevels[4]);
    }
}
