package com.distributedsystems.consistenthashing.storage;

import com.distributedsystems.consistenthashing.model.NodeStatus;
import com.distributedsystems.consistenthashing.model.PhysicalNode;
import com.distributedsystems.consistenthashing.model.VersionedValue;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Storage node instance managing local key-value state and supporting
 * chaos engineering fault injection (simulated latencies, forced node crashes).
 */
public class StorageNode {

    private final PhysicalNode physicalNode;
    private final KeyValueStore store;
    private volatile long simulatedLatencyMillis;
    private volatile boolean forcedFailure;

    private final AtomicLong getCount = new AtomicLong(0);
    private final AtomicLong putCount = new AtomicLong(0);
    private final AtomicLong deleteCount = new AtomicLong(0);

    public StorageNode(PhysicalNode physicalNode) {
        this(physicalNode, new InMemoryStorageEngine());
    }

    public StorageNode(PhysicalNode physicalNode, KeyValueStore store) {
        if (physicalNode == null) {
            throw new IllegalArgumentException("PhysicalNode cannot be null");
        }
        if (store == null) {
            throw new IllegalArgumentException("KeyValueStore cannot be null");
        }
        this.physicalNode = physicalNode;
        this.store = store;
        this.simulatedLatencyMillis = 0;
        this.forcedFailure = false;
    }

    public PhysicalNode getPhysicalNode() {
        return physicalNode;
    }

    public String getId() {
        return physicalNode.getId();
    }

    public KeyValueStore getStore() {
        return store;
    }

    public VersionedValue get(String key) {
        checkFailureAndDelay();
        getCount.incrementAndGet();
        return store.get(key);
    }

    public VersionedValue getRaw(String key) {
        checkFailureAndDelay();
        getCount.incrementAndGet();
        return store.getRaw(key);
    }

    public void put(String key, VersionedValue value) {
        checkFailureAndDelay();
        putCount.incrementAndGet();
        store.put(key, value);
    }

    public void delete(String key, VersionedValue tombstone) {
        checkFailureAndDelay();
        deleteCount.incrementAndGet();
        store.delete(key, tombstone);
    }

    public Map<String, VersionedValue> getAll() {
        return store.getAll();
    }

    public int keyCount() {
        return store.size();
    }

    public void clear() {
        store.clear();
    }

    // --- Fault Injection & Simulation Controls ---

    public void setSimulatedLatencyMillis(long millis) {
        this.simulatedLatencyMillis = millis;
    }

    public void simulateCrash() {
        this.forcedFailure = true;
        this.physicalNode.setStatus(NodeStatus.DEAD);
    }

    public void recover() {
        this.forcedFailure = false;
        this.physicalNode.setStatus(NodeStatus.HEALTHY);
    }

    public boolean isHealthy() {
        return !forcedFailure && physicalNode.isAvailable();
    }

    private void checkFailureAndDelay() {
        if (forcedFailure || !physicalNode.isAvailable()) {
            throw new NodeUnavailableException("Node " + physicalNode.getId() + " is currently offline/crashed");
        }
        if (simulatedLatencyMillis > 0) {
            try {
                Thread.sleep(simulatedLatencyMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Node operation interrupted", e);
            }
        }
    }

    public long getGetCount() {
        return getCount.get();
    }

    public long getPutCount() {
        return putCount.get();
    }

    public long getDeleteCount() {
        return deleteCount.get();
    }

    @Override
    public String toString() {
        return "StorageNode{" +
                "id='" + physicalNode.getId() + '\'' +
                ", keys=" + store.size() +
                ", healthy=" + isHealthy() +
                '}';
    }
}
