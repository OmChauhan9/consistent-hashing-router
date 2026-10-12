package com.distributedsystems.consistenthashing.storage;

import com.distributedsystems.consistenthashing.model.VersionedValue;

import java.util.Map;

/**
 * Key-Value storage abstraction implemented by individual storage engine replicas.
 */
public interface KeyValueStore {

    /**
     * Retrieves the versioned value for a key, or null if non-existent or tombstoned.
     */
    VersionedValue get(String key);

    /**
     * Retrieves the raw VersionedValue including tombstones for replication reconciliation.
     */
    VersionedValue getRaw(String key);

    /**
     * Inserts or updates a versioned value.
     */
    void put(String key, VersionedValue value);

    /**
     * Marks a key as deleted by writing a tombstone version.
     */
    void delete(String key, VersionedValue tombstone);

    /**
     * Checks if a non-tombstoned entry exists for this key.
     */
    boolean containsKey(String key);

    /**
     * Returns an immutable snapshot of all live key-value pairs.
     */
    Map<String, VersionedValue> getAll();

    /**
     * Count of live (non-tombstoned) keys.
     */
    int size();

    /**
     * Clears all stored data.
     */
    void clear();
}
