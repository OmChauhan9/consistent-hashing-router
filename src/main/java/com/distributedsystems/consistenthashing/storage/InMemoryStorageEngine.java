package com.distributedsystems.consistenthashing.storage;

import com.distributedsystems.consistenthashing.model.VersionedValue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe in-memory key-value storage engine using {@link ConcurrentHashMap}.
 * Automatically reconciles concurrent or out-of-order writes using Vector Clocks and LWW.
 */
public class InMemoryStorageEngine implements KeyValueStore {

    private final ConcurrentHashMap<String, VersionedValue> store;

    public InMemoryStorageEngine() {
        this.store = new ConcurrentHashMap<>();
    }

    @Override
    public VersionedValue get(String key) {
        if (key == null) return null;
        VersionedValue entry = store.get(key);
        if (entry == null || entry.isTombstone()) {
            return null;
        }
        return entry;
    }

    @Override
    public VersionedValue getRaw(String key) {
        if (key == null) return null;
        return store.get(key);
    }

    @Override
    public void put(String key, VersionedValue incoming) {
        if (key == null || incoming == null) {
            throw new IllegalArgumentException("Key and value must not be null");
        }
        store.compute(key, (k, existing) -> {
            if (existing == null) {
                return incoming;
            }
            return VersionedValue.reconcile(existing, incoming);
        });
    }

    @Override
    public void delete(String key, VersionedValue tombstone) {
        if (key == null || tombstone == null) {
            throw new IllegalArgumentException("Key and tombstone must not be null");
        }
        store.compute(key, (k, existing) -> {
            if (existing == null) {
                return tombstone;
            }
            return VersionedValue.reconcile(existing, tombstone);
        });
    }

    @Override
    public boolean containsKey(String key) {
        if (key == null) return false;
        VersionedValue val = store.get(key);
        return val != null && !val.isTombstone();
    }

    @Override
    public Map<String, VersionedValue> getAll() {
        Map<String, VersionedValue> liveMap = new HashMap<>();
        for (Map.Entry<String, VersionedValue> entry : store.entrySet()) {
            if (!entry.getValue().isTombstone()) {
                liveMap.put(entry.getKey(), entry.getValue());
            }
        }
        return Collections.unmodifiableMap(liveMap);
    }

    @Override
    public int size() {
        int count = 0;
        for (VersionedValue v : store.values()) {
            if (!v.isTombstone()) {
                count++;
            }
        }
        return count;
    }

    @Override
    public void clear() {
        store.clear();
    }
}
