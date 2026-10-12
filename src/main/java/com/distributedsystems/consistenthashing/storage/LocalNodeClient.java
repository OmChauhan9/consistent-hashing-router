package com.distributedsystems.consistenthashing.storage;

import com.distributedsystems.consistenthashing.model.PhysicalNode;
import com.distributedsystems.consistenthashing.model.VersionedValue;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

/**
 * In-process implementation of {@link NodeClient} providing asynchronous non-blocking
 * request dispatch to local StorageNode instances. Ideal for high-speed simulation,
 * unit tests, and performance benchmarks.
 */
public class LocalNodeClient implements NodeClient {

    private final Map<String, StorageNode> nodeRegistry;
    private final Executor executor;

    public LocalNodeClient() {
        this(ForkJoinPool.commonPool());
    }

    public LocalNodeClient(Executor executor) {
        this.nodeRegistry = new ConcurrentHashMap<>();
        this.executor = executor != null ? executor : ForkJoinPool.commonPool();
    }

    public void registerNode(StorageNode node) {
        if (node != null) {
            nodeRegistry.put(node.getId(), node);
        }
    }

    public void unregisterNode(String nodeId) {
        nodeRegistry.remove(nodeId);
    }

    public StorageNode getNode(String nodeId) {
        return nodeRegistry.get(nodeId);
    }

    private StorageNode resolveNode(PhysicalNode node) {
        if (node == null) {
            throw new IllegalArgumentException("PhysicalNode must not be null");
        }
        StorageNode target = nodeRegistry.get(node.getId());
        if (target == null) {
            throw new NodeUnavailableException("Node " + node.getId() + " is not registered in local transport");
        }
        return target;
    }

    @Override
    public CompletableFuture<VersionedValue> get(PhysicalNode node, String key) {
        return CompletableFuture.supplyAsync(() -> {
            StorageNode target = resolveNode(node);
            return target.get(key);
        }, executor);
    }

    @Override
    public CompletableFuture<VersionedValue> getRaw(PhysicalNode node, String key) {
        return CompletableFuture.supplyAsync(() -> {
            StorageNode target = resolveNode(node);
            return target.getRaw(key);
        }, executor);
    }

    @Override
    public CompletableFuture<Void> put(PhysicalNode node, String key, VersionedValue value) {
        return CompletableFuture.runAsync(() -> {
            StorageNode target = resolveNode(node);
            target.put(key, value);
        }, executor);
    }

    @Override
    public CompletableFuture<Void> delete(PhysicalNode node, String key, VersionedValue tombstone) {
        return CompletableFuture.runAsync(() -> {
            StorageNode target = resolveNode(node);
            target.delete(key, tombstone);
        }, executor);
    }

    @Override
    public CompletableFuture<Boolean> ping(PhysicalNode node) {
        return CompletableFuture.supplyAsync(() -> {
            StorageNode target = nodeRegistry.get(node.getId());
            return target != null && target.isHealthy();
        }, executor);
    }
}
