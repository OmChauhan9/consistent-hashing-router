package com.distributedsystems.consistenthashing.cluster;

import com.distributedsystems.consistenthashing.model.PhysicalNode;

import java.util.Collections;
import java.util.List;

/**
 * Migration plan describing key ranges and physical node transfers
 * triggered by ring membership changes (node join/leave).
 */
public class MigrationPlan {

    public static class MigrationTask {
        private final PhysicalNode sourceNode;
        private final PhysicalNode targetNode;
        private final List<String> keysToMigrate;

        public MigrationTask(PhysicalNode sourceNode, PhysicalNode targetNode, List<String> keysToMigrate) {
            this.sourceNode = sourceNode;
            this.targetNode = targetNode;
            this.keysToMigrate = keysToMigrate != null ? List.copyOf(keysToMigrate) : Collections.emptyList();
        }

        public PhysicalNode getSourceNode() {
            return sourceNode;
        }

        public PhysicalNode getTargetNode() {
            return targetNode;
        }

        public List<String> getKeysToMigrate() {
            return keysToMigrate;
        }

        public int getKeyCount() {
            return keysToMigrate.size();
        }

        @Override
        public String toString() {
            return String.format("Transfer %d keys from [%s] -> [%s]",
                    keysToMigrate.size(), sourceNode.getId(), targetNode.getId());
        }
    }

    private final List<MigrationTask> tasks;
    private final int totalKeysInCluster;
    private final int totalKeysMigrated;
    private final double migrationPercentage;

    public MigrationPlan(List<MigrationTask> tasks, int totalKeysInCluster) {
        this.tasks = tasks != null ? List.copyOf(tasks) : Collections.emptyList();
        this.totalKeysInCluster = totalKeysInCluster;
        this.totalKeysMigrated = this.tasks.stream().mapToInt(MigrationTask::getKeyCount).sum();
        this.migrationPercentage = totalKeysInCluster > 0
                ? (this.totalKeysMigrated * 100.0) / totalKeysInCluster
                : 0.0;
    }

    public List<MigrationTask> getTasks() {
        return tasks;
    }

    public int getTotalKeysInCluster() {
        return totalKeysInCluster;
    }

    public int getTotalKeysMigrated() {
        return totalKeysMigrated;
    }

    public double getMigrationPercentage() {
        return migrationPercentage;
    }

    @Override
    public String toString() {
        return String.format("MigrationPlan: %d/%d keys relocated (%.2f%%). Tasks: %d",
                totalKeysMigrated, totalKeysInCluster, migrationPercentage, tasks.size());
    }
}
