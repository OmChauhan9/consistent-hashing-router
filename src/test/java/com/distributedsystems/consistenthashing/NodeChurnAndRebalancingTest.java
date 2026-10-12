package com.distributedsystems.consistenthashing;

import com.distributedsystems.consistenthashing.cluster.MigrationPlan;
import com.distributedsystems.consistenthashing.cluster.PartitionRebalancer;
import com.distributedsystems.consistenthashing.hash.Murmur3Hash;
import com.distributedsystems.consistenthashing.model.PhysicalNode;
import com.distributedsystems.consistenthashing.model.VersionedValue;
import com.distributedsystems.consistenthashing.ring.ConsistentHashRing;
import com.distributedsystems.consistenthashing.simulation.NodeChurnSimulator;
import com.distributedsystems.consistenthashing.storage.StorageNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class NodeChurnAndRebalancingTest {

    @Test
    @DisplayName("Node addition rebalances ~K/(N+1) keys in Consistent Hashing vs >85% in Modulo Hashing")
    void shouldRelocateOnlyMinimalKeysOnNodeAddition() {
        int keyCount = 50_000;
        List<String> keys = new ArrayList<>(keyCount);
        for (int i = 0; i < keyCount; i++) {
            keys.add("telemetry_device_" + i);
        }

        int initialNodes = 10;
        int vnodes = 150;

        NodeChurnSimulator.ChurnComparisonResult result =
                NodeChurnSimulator.simulateNodeAddition(initialNodes, vnodes, keys);

        // Theoretical optimal movement is 1/(10 + 1) = 9.09%
        double theoretical = 100.0 / 11.0;
        assertThat(result.getConsistentHashingPercentMoved())
                .isCloseTo(theoretical, within(3.5));

        // Modulo hashing suffers catastrophic data storm (>85% moved)
        assertThat(result.getModuloHashingPercentMoved()).isGreaterThan(85.0);

        System.out.println("Node Addition Test Result:\n" + result);
    }

    @Test
    @DisplayName("Node removal rebalances ~K/N keys in Consistent Hashing vs >85% in Modulo Hashing")
    void shouldRelocateOnlyMinimalKeysOnNodeRemoval() {
        int keyCount = 50_000;
        List<String> keys = new ArrayList<>(keyCount);
        for (int i = 0; i < keyCount; i++) {
            keys.add("account_transaction_" + i);
        }

        int initialNodes = 10;
        int vnodes = 150;

        NodeChurnSimulator.ChurnComparisonResult result =
                NodeChurnSimulator.simulateNodeRemoval(initialNodes, vnodes, keys);

        // Theoretical optimal movement on removing 1 of 10 nodes is 10.0%
        assertThat(result.getConsistentHashingPercentMoved())
                .isCloseTo(10.0, within(3.5));

        // Modulo hashing relocates almost everything (>85%)
        assertThat(result.getModuloHashingPercentMoved()).isGreaterThan(85.0);

        System.out.println("Node Removal Test Result:\n" + result);
    }

    @Test
    @DisplayName("Untouched nodes in Consistent Hashing never shuffle keys between each other")
    void shouldNotMoveKeysBetweenUntouchedNodes() {
        ConsistentHashRing ring = new ConsistentHashRing(new Murmur3Hash(), 100);
        for (int i = 1; i <= 5; i++) {
            ring.addPhysicalNode(new PhysicalNode("node-" + i), 100);
        }

        int keyCount = 10_000;
        Map<String, String> initialOwnership = new HashMap<>();
        for (int i = 0; i < keyCount; i++) {
            String key = "test_key_" + i;
            initialOwnership.put(key, ring.getPrimaryNode(key).getId());
        }

        // Add 6th node
        PhysicalNode node6 = new PhysicalNode("node-6");
        ring.addPhysicalNode(node6, 100);

        // Check ownership post-addition
        for (int i = 0; i < keyCount; i++) {
            String key = "test_key_" + i;
            String originalNode = initialOwnership.get(key);
            String currentNode = ring.getPrimaryNode(key).getId();

            if (!currentNode.equals(originalNode)) {
                // If a key moved, it MUST have moved to the new node-6, NEVER between existing nodes!
                assertThat(currentNode).isEqualTo("node-6");
            }
        }
    }

    @Test
    @DisplayName("PartitionRebalancer correctly computes migration plan and transfers data")
    void shouldPlanAndExecutePartitionMigration() {
        ConsistentHashRing ring = new ConsistentHashRing(new Murmur3Hash(), 100);
        Map<String, StorageNode> storageNodes = new HashMap<>();

        for (int i = 1; i <= 4; i++) {
            PhysicalNode pNode = new PhysicalNode("node-" + i);
            StorageNode sNode = new StorageNode(pNode);
            storageNodes.put(pNode.getId(), sNode);
            ring.addPhysicalNode(pNode, 100);
        }

        // Populate initial data across nodes
        for (int i = 0; i < 2000; i++) {
            String key = "customer_" + i;
            PhysicalNode primary = ring.getPrimaryNode(key);
            storageNodes.get(primary.getId()).put(key, VersionedValue.of("data_" + i, "seed"));
        }

        // Add 5th node
        PhysicalNode node5 = new PhysicalNode("node-5");
        StorageNode sNode5 = new StorageNode(node5);
        storageNodes.put(node5.getId(), sNode5);
        ring.addPhysicalNode(node5, 100);

        // Compute migration plan
        MigrationPlan plan = PartitionRebalancer.planNodeAddition(ring, storageNodes, node5);

        // Expected migration should be ~20% (1/5 of cluster)
        assertThat(plan.getMigrationPercentage()).isCloseTo(20.0, within(6.0));
        assertThat(plan.getTasks()).isNotEmpty();

        // Execute migration
        PartitionRebalancer.executeMigration(plan, storageNodes, false);

        // Verify newly joined node now holds the migrated keys
        assertThat(sNode5.keyCount()).isEqualTo(plan.getTotalKeysMigrated());
    }
}
