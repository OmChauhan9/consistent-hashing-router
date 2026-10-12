package com.distributedsystems.consistenthashing.cluster;

import com.distributedsystems.consistenthashing.hash.HashFunction;
import com.distributedsystems.consistenthashing.hash.Murmur3Hash;
import com.distributedsystems.consistenthashing.model.PhysicalNode;
import com.distributedsystems.consistenthashing.quorum.QuorumConfig;
import com.distributedsystems.consistenthashing.quorum.QuorumCoordinator;
import com.distributedsystems.consistenthashing.quorum.QuorumReadResult;
import com.distributedsystems.consistenthashing.quorum.QuorumWriteResult;
import com.distributedsystems.consistenthashing.ring.ConsistentHashRing;
import com.distributedsystems.consistenthashing.storage.LocalNodeClient;
import com.distributedsystems.consistenthashing.storage.StorageNode;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Top-level manager orchestrating cluster lifecycle, node membership changes,
 * hash ring updates, storage engine backends, and quorum client operations.
 */
public class ClusterManager {

    private final ConsistentHashRing ring;
    private final LocalNodeClient transport;
    private final Map<String, StorageNode> storageNodes;
    private final QuorumCoordinator coordinator;
    private final int defaultVnodesPerNode;

    public ClusterManager() {
        this(new Murmur3Hash(), 100, QuorumConfig.defaultDynamo());
    }

    public ClusterManager(HashFunction hashFunction, int defaultVnodes, QuorumConfig quorumConfig) {
        this.defaultVnodesPerNode = defaultVnodes;
        this.ring = new ConsistentHashRing(hashFunction, defaultVnodes);
        this.transport = new LocalNodeClient();
        this.storageNodes = new ConcurrentHashMap<>();
        this.coordinator = new QuorumCoordinator(this.ring, this.transport, quorumConfig, "cluster-coordinator");
    }

    /**
     * Registers a new physical node into the cluster, provisions its local storage engine,
     * populates its vnodes on the ring, and attaches it to the transport layer.
     */
    public StorageNode addNode(PhysicalNode node) {
        return addNode(node, defaultVnodesPerNode);
    }

    /**
     * Registers a new physical node with custom vnodes count.
     */
    public StorageNode addNode(PhysicalNode node, int vnodes) {
        if (node == null) {
            throw new IllegalArgumentException("PhysicalNode must not be null");
        }

        StorageNode storageNode = new StorageNode(node);
        storageNodes.put(node.getId(), storageNode);
        transport.registerNode(storageNode);
        ring.addPhysicalNode(node, vnodes);
        return storageNode;
    }

    /**
     * Removes a physical node from the cluster and ring.
     */
    public boolean removeNode(String nodeId) {
        StorageNode storageNode = storageNodes.remove(nodeId);
        if (storageNode == null) {
            return false;
        }

        transport.unregisterNode(nodeId);
        return ring.removePhysicalNode(storageNode.getPhysicalNode());
    }

    /**
     * Executes a client write request through the Quorum Coordinator.
     */
    public QuorumWriteResult put(String key, String value) {
        return coordinator.put(key, value);
    }

    /**
     * Executes a client read request through the Quorum Coordinator.
     */
    public QuorumReadResult get(String key) {
        return coordinator.get(key);
    }

    /**
     * Executes a client delete request through the Quorum Coordinator.
     */
    public QuorumWriteResult delete(String key) {
        return coordinator.delete(key);
    }

    /**
     * Simulates a hard crash on a specific node for chaos testing.
     */
    public void crashNode(String nodeId) {
        StorageNode node = storageNodes.get(nodeId);
        if (node != null) {
            node.simulateCrash();
        }
    }

    /**
     * Restores a crashed node back to operational health.
     */
    public void recoverNode(String nodeId) {
        StorageNode node = storageNodes.get(nodeId);
        if (node != null) {
            node.recover();
        }
    }

    public ConsistentHashRing getRing() {
        return ring;
    }

    public QuorumCoordinator getCoordinator() {
        return coordinator;
    }

    public StorageNode getStorageNode(String nodeId) {
        return storageNodes.get(nodeId);
    }

    public Map<String, StorageNode> getStorageNodes() {
        return Collections.unmodifiableMap(storageNodes);
    }

    public int getNodeCount() {
        return storageNodes.size();
    }

    public int getTotalStoredKeysAcrossCluster() {
        int total = 0;
        for (StorageNode node : storageNodes.values()) {
            total += node.keyCount();
        }
        return total;
    }
}
