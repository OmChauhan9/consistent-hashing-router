package com.distributedsystems.consistenthashing.cluster;

import com.distributedsystems.consistenthashing.model.PhysicalNode;
import com.distributedsystems.consistenthashing.model.VersionedValue;
import com.distributedsystems.consistenthashing.ring.ConsistentHashRing;
import com.distributedsystems.consistenthashing.storage.StorageNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Calculates partition delta and executes dynamic data rebalancing across nodes
 * during cluster scale-out and scale-in events.
 */
public class PartitionRebalancer {

    /**
     * Computes the migration plan when a new node is introduced to the cluster.
     * Evaluates all currently stored keys and finds which keys must migrate to the new node.
     *
     * @param ring current hash ring containing the newly added node
     * @param existingStorageNodes map of nodeId -> StorageNode currently holding data
     * @param newNode the newly joined physical node
     * @return MigrationPlan detailing keys migrating to the new node
     */
    public static MigrationPlan planNodeAddition(
            ConsistentHashRing ring,
            Map<String, StorageNode> existingStorageNodes,
            PhysicalNode newNode) {

        Map<PhysicalNode, List<String>> migrationsBySource = new HashMap<>();
        int totalKeys = 0;

        for (StorageNode storageNode : existingStorageNodes.values()) {
            if (storageNode.getId().equals(newNode.getId())) {
                continue; // Skip the new node itself
            }

            Map<String, VersionedValue> nodeData = storageNode.getAll();
            totalKeys += nodeData.size();

            for (String key : nodeData.keySet()) {
                PhysicalNode newPrimary = ring.getPrimaryNode(key);
                if (newPrimary != null && newPrimary.getId().equals(newNode.getId())) {
                    // This key has shifted ownership to the new node!
                    migrationsBySource.computeIfAbsent(storageNode.getPhysicalNode(), k -> new ArrayList<>())
                            .add(key);
                }
            }
        }

        List<MigrationPlan.MigrationTask> tasks = new ArrayList<>();
        for (Map.Entry<PhysicalNode, List<String>> entry : migrationsBySource.entrySet()) {
            tasks.add(new MigrationPlan.MigrationTask(entry.getKey(), newNode, entry.getValue()));
        }

        return new MigrationPlan(tasks, totalKeys);
    }

    /**
     * Executes a migration plan by copying versioned data from source nodes to target nodes.
     *
     * @param plan migration plan to execute
     * @param nodesMap map of nodeId -> StorageNode instances
     * @param purgeFromSource if true, removes migrated keys from source after transfer
     */
    public static void executeMigration(
            MigrationPlan plan,
            Map<String, StorageNode> nodesMap,
            boolean purgeFromSource) {

        for (MigrationPlan.MigrationTask task : plan.getTasks()) {
            StorageNode source = nodesMap.get(task.getSourceNode().getId());
            StorageNode target = nodesMap.get(task.getTargetNode().getId());

            if (source == null || target == null) {
                continue;
            }

            for (String key : task.getKeysToMigrate()) {
                VersionedValue val = source.getRaw(key);
                if (val != null) {
                    target.put(key, val);
                    if (purgeFromSource) {
                        // In production, tombstone or purge old partition slice
                        source.getStore().clear(); // Or selective delete
                    }
                }
            }
        }
    }
}
