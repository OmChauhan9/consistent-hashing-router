package com.distributedsystems.consistenthashing.storage;

import com.distributedsystems.consistenthashing.model.PhysicalNode;
import com.distributedsystems.consistenthashing.model.VersionedValue;

import java.util.concurrent.CompletableFuture;

/**
 * Transport abstraction for dispatching asynchronous network/storage requests
 * from the Quorum Coordinator to individual Physical Nodes.
 */
public interface NodeClient {

    /**
     * Reads a value asynchronously from the target physical node.
     */
    CompletableFuture<VersionedValue> get(PhysicalNode node, String key);

    /**
     * Reads raw versioned value (including tombstones) for replication repair.
     */
    CompletableFuture<VersionedValue> getRaw(PhysicalNode node, String key);

    /**
     * Writes a versioned value asynchronously to the target physical node.
     */
    CompletableFuture<Void> put(PhysicalNode node, String key, VersionedValue value);

    /**
     * Deletes a key asynchronously on the target physical node via a tombstone.
     */
    CompletableFuture<Void> delete(PhysicalNode node, String key, VersionedValue tombstone);

    /**
     * Health check probe to the target physical node.
     */
    CompletableFuture<Boolean> ping(PhysicalNode node);
}
