package com.distributedsystems.consistenthashing;

import com.distributedsystems.consistenthashing.hash.Murmur3Hash;
import com.distributedsystems.consistenthashing.hash.Sha256Hash;
import com.distributedsystems.consistenthashing.model.PhysicalNode;
import com.distributedsystems.consistenthashing.ring.ConsistentHashRing;
import com.distributedsystems.consistenthashing.ring.PreferenceList;
import com.distributedsystems.consistenthashing.ring.RingSnapshot;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class ConsistentHashRingTest {

    @Test
    @DisplayName("Ring correctly registers physical nodes and populates V virtual tokens")
    void shouldRegisterPhysicalNodesWithVirtualNodes() {
        ConsistentHashRing ring = new ConsistentHashRing(new Murmur3Hash(), 50);

        PhysicalNode node1 = new PhysicalNode("node-1");
        PhysicalNode node2 = new PhysicalNode("node-2");

        ring.addPhysicalNode(node1, 50);
        ring.addPhysicalNode(node2, 50);

        assertThat(ring.getPhysicalNodeCount()).isEqualTo(2);
        assertThat(ring.getVirtualNodeCount()).isEqualTo(100);

        RingSnapshot snapshot = ring.getRingSnapshot();
        assertThat(snapshot.getVnodeCountPerNode().get(node1)).isEqualTo(50);
        assertThat(snapshot.getVnodeCountPerNode().get(node2)).isEqualTo(50);
    }

    @Test
    @DisplayName("Ring supports heterogeneous hardware weighting")
    void shouldSupportHeterogeneousNodeWeights() {
        ConsistentHashRing ring = new ConsistentHashRing(new Murmur3Hash(), 100);

        PhysicalNode smallNode = new PhysicalNode("node-small", "localhost", 8001, 1.0);
        PhysicalNode largeNode = new PhysicalNode("node-large", "localhost", 8002, 3.0); // 3x capacity

        ring.addPhysicalNode(smallNode, 100);
        ring.addPhysicalNode(largeNode, 100);

        assertThat(ring.getVirtualNodeCount()).isEqualTo(400); // 100 + 300
        RingSnapshot snapshot = ring.getRingSnapshot();
        assertThat(snapshot.getVnodeCountPerNode().get(smallNode)).isEqualTo(100);
        assertThat(snapshot.getVnodeCountPerNode().get(largeNode)).isEqualTo(300);

        // Arc coverage for large node should be approximately 75%
        double largeArc = snapshot.getArcCoveragePercentPerNode().get(largeNode);
        assertThat(largeArc).isCloseTo(75.0, within(5.0));
    }

    @Test
    @DisplayName("Clockwise traversal wraps around on ring boundary")
    void shouldWrapAroundClockwise() {
        ConsistentHashRing ring = new ConsistentHashRing(new Murmur3Hash(), 10);
        PhysicalNode nodeA = new PhysicalNode("node-A");
        ring.addPhysicalNode(nodeA);

        // For any key, the primary coordinator must resolve to nodeA on a single-node ring
        for (int i = 0; i < 50; i++) {
            PhysicalNode primary = ring.getPrimaryNode("test_key_" + i);
            assertThat(primary).isEqualTo(nodeA);
        }
    }

    @Test
    @DisplayName("Preference list guarantees R distinct physical machines avoiding colocation")
    void shouldAvoidPhysicalColocationInPreferenceList() {
        ConsistentHashRing ring = new ConsistentHashRing(new Murmur3Hash(), 100);
        for (int i = 1; i <= 5; i++) {
            ring.addPhysicalNode(new PhysicalNode("server-0" + i), 100);
        }

        // Test across 500 diverse keys
        for (int k = 0; k < 500; k++) {
            String key = "session_uuid_" + k;
            PreferenceList prefList = ring.getPreferenceList(key, 3);

            assertThat(prefList.size()).isEqualTo(3);

            // Assert that all 3 nodes in preference list are distinct physical machines
            List<PhysicalNode> nodes = prefList.getNodes();
            Set<String> uniqueIds = new HashSet<>();
            for (PhysicalNode node : nodes) {
                uniqueIds.add(node.getId());
            }

            assertThat(uniqueIds).hasSize(3);
        }
    }

    @Test
    @DisplayName("Removing a physical node cleans up all its virtual nodes")
    void shouldRemoveAllVirtualNodesOnNodeRemoval() {
        ConsistentHashRing ring = new ConsistentHashRing(new Murmur3Hash(), 50);
        PhysicalNode node1 = new PhysicalNode("node-1");
        PhysicalNode node2 = new PhysicalNode("node-2");

        ring.addPhysicalNode(node1, 50);
        ring.addPhysicalNode(node2, 50);
        assertThat(ring.getVirtualNodeCount()).isEqualTo(100);

        boolean removed = ring.removePhysicalNode(node1);
        assertThat(removed).isTrue();
        assertThat(ring.getPhysicalNodeCount()).isEqualTo(1);
        assertThat(ring.getVirtualNodeCount()).isEqualTo(50);
        assertThat(ring.containsPhysicalNode(node1)).isFalse();
        assertThat(ring.containsPhysicalNode(node2)).isTrue();
    }

    @Test
    @DisplayName("Arc coverage percentages across all physical nodes sum to 100%")
    void shouldSumToHundredPercentArcCoverage() {
        ConsistentHashRing ring = new ConsistentHashRing(new Murmur3Hash(), 100);
        for (int i = 1; i <= 6; i++) {
            ring.addPhysicalNode(new PhysicalNode("node-" + i), 100);
        }

        RingSnapshot snapshot = ring.getRingSnapshot();
        double totalCoverage = snapshot.getArcCoveragePercentPerNode().values().stream()
                .mapToDouble(Double::doubleValue)
                .sum();

        assertThat(totalCoverage).isCloseTo(100.0, within(0.01));
    }

    @Test
    @DisplayName("Works equivalently with cryptographic SHA-256 hash function")
    void shouldWorkWithSha256() {
        ConsistentHashRing ring = new ConsistentHashRing(new Sha256Hash(), 50);
        PhysicalNode nodeA = new PhysicalNode("node-A");
        PhysicalNode nodeB = new PhysicalNode("node-B");

        ring.addPhysicalNode(nodeA);
        ring.addPhysicalNode(nodeB);

        PreferenceList prefList = ring.getPreferenceList("crypto_key_test", 2);
        assertThat(prefList.size()).isEqualTo(2);
        assertThat(prefList.getNodes().get(0)).isNotEqualTo(prefList.getNodes().get(1));
    }
}
