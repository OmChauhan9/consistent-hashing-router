package com.distributedsystems.consistenthashing.ring;

import com.distributedsystems.consistenthashing.hash.HashFunction;
import com.distributedsystems.consistenthashing.hash.Murmur3Hash;
import com.distributedsystems.consistenthashing.model.PhysicalNode;
import com.distributedsystems.consistenthashing.model.VirtualNode;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Thread-safe implementation of a Consistent Hash Ring with Virtual Nodes (vnodes)
 * and Partitioned Replication Routing.
 *
 * <p>Uses a {@link java.util.TreeMap} for O(log N) ceiling token lookups,
 * with clockwise circular wrap-around to index 0 on ring exhaustion.</p>
 */
public class ConsistentHashRing {

    private final HashFunction hashFunction;
    private final int defaultVnodesPerNode;
    private final TreeMap<Long, VirtualNode> ring;
    private final Map<PhysicalNode, Set<Long>> physicalToTokensMap;
    private final ReentrantReadWriteLock rwLock;

    public ConsistentHashRing() {
        this(new Murmur3Hash(), 100);
    }

    public ConsistentHashRing(HashFunction hashFunction, int defaultVnodesPerNode) {
        if (hashFunction == null) {
            throw new IllegalArgumentException("HashFunction cannot be null");
        }
        if (defaultVnodesPerNode <= 0) {
            throw new IllegalArgumentException("Default vnodes must be > 0");
        }
        this.hashFunction = hashFunction;
        this.defaultVnodesPerNode = defaultVnodesPerNode;
        this.ring = new TreeMap<>();
        this.physicalToTokensMap = new HashMap<>();
        this.rwLock = new ReentrantReadWriteLock();
    }

    /**
     * Registers a physical node using the default virtual node count, scaled by the node's capacity weight.
     *
     * @param node physical node to register
     */
    public void addPhysicalNode(PhysicalNode node) {
        addPhysicalNode(node, defaultVnodesPerNode);
    }

    /**
     * Registers a physical node with an explicit base virtual node count, scaled by node weight.
     *
     * @param node physical node to register
     * @param baseVnodes base number of virtual node points
     */
    public void addPhysicalNode(PhysicalNode node, int baseVnodes) {
        if (node == null) {
            throw new IllegalArgumentException("PhysicalNode cannot be null");
        }
        if (baseVnodes <= 0) {
            throw new IllegalArgumentException("Base vnodes must be > 0");
        }

        rwLock.writeLock().lock();
        try {
            // Remove existing tokens if node was previously added
            removePhysicalNodeInternal(node);

            int effectiveVnodes = (int) Math.max(1, Math.round(baseVnodes * node.getWeight()));
            Set<Long> allocatedTokens = new HashSet<>(effectiveVnodes);

            for (int i = 0; i < effectiveVnodes; i++) {
                String tokenString = node.getId() + "#VN#" + i;
                long token = hashFunction.hash(tokenString.getBytes(StandardCharsets.UTF_8));

                // Handle collisions by probing linearly
                while (ring.containsKey(token)) {
                    token = (token + 1) & hashFunction.maxHashValue();
                }

                VirtualNode vNode = new VirtualNode(node, i, token);
                ring.put(token, vNode);
                allocatedTokens.add(token);
            }

            physicalToTokensMap.put(node, allocatedTokens);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Removes a physical node and all of its associated virtual nodes from the ring.
     *
     * @param node physical node to unregister
     * @return true if node was present and removed
     */
    public boolean removePhysicalNode(PhysicalNode node) {
        if (node == null) {
            return false;
        }

        rwLock.writeLock().lock();
        try {
            return removePhysicalNodeInternal(node);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    private boolean removePhysicalNodeInternal(PhysicalNode node) {
        Set<Long> tokens = physicalToTokensMap.remove(node);
        if (tokens == null) {
            return false;
        }
        for (Long token : tokens) {
            ring.remove(token);
        }
        return true;
    }

    /**
     * Resolves the primary coordinator physical node for a given string key.
     */
    public PhysicalNode getPrimaryNode(String key) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        return getPrimaryNode(key.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Resolves the primary coordinator physical node for raw key bytes.
     */
    public PhysicalNode getPrimaryNode(byte[] key) {
        PreferenceList prefList = getPreferenceList(key, 1);
        if (prefList.size() == 0) {
            return null;
        }
        return prefList.getCoordinator();
    }

    /**
     * Resolves the replication preference list for a given string key.
     * Guarantees R distinct physical hosts by traversing clockwise and skipping
     * collocated virtual nodes.
     *
     * @param key client key
     * @param replicationFactor number of distinct physical replicas desired (R)
     * @return PreferenceList containing ordered distinct physical replicas
     */
    public PreferenceList getPreferenceList(String key, int replicationFactor) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        return getPreferenceList(key.getBytes(StandardCharsets.UTF_8), replicationFactor, key);
    }

    /**
     * Resolves the replication preference list for raw key bytes.
     */
    public PreferenceList getPreferenceList(byte[] key, int replicationFactor) {
        return getPreferenceList(key, replicationFactor, new String(key, StandardCharsets.UTF_8));
    }

    private PreferenceList getPreferenceList(byte[] keyBytes, int replicationFactor, String keyString) {
        if (keyBytes == null) {
            throw new IllegalArgumentException("Key bytes cannot be null");
        }
        if (replicationFactor <= 0) {
            throw new IllegalArgumentException("Replication factor must be > 0");
        }

        rwLock.readLock().lock();
        try {
            if (ring.isEmpty()) {
                return new PreferenceList(keyString, 0L, 0L, Collections.emptyList());
            }

            long keyHash = hashFunction.hash(keyBytes);

            // Find first token clockwise >= keyHash
            Long startToken = ring.ceilingKey(keyHash);
            if (startToken == null) {
                startToken = ring.firstKey(); // Circular wrap-around
            }

            int targetReplicas = Math.min(replicationFactor, physicalToTokensMap.size());
            List<PhysicalNode> replicas = new ArrayList<>(targetReplicas);
            Set<String> visitedPhysicalNodeIds = new HashSet<>(targetReplicas);

            Long currentToken = startToken;
            long primaryToken = startToken;

            while (replicas.size() < targetReplicas && visitedPhysicalNodeIds.size() < physicalToTokensMap.size()) {
                VirtualNode vNode = ring.get(currentToken);
                PhysicalNode pNode = vNode.getPhysicalNode();

                // Physical colocation avoidance: only collect distinct physical machines
                if (visitedPhysicalNodeIds.add(pNode.getId())) {
                    replicas.add(pNode);
                }

                // Advance clockwise
                currentToken = ring.higherKey(currentToken);
                if (currentToken == null) {
                    currentToken = ring.firstKey(); // Wrap-around
                }

                // Guard against infinite loop if traversed complete circle
                if (Objects.equals(currentToken, startToken)) {
                    break;
                }
            }

            return new PreferenceList(keyString, keyHash, primaryToken, replicas);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Calculates the exact mathematical arc coverage percentage of the hash ring
     * owned by each physical node across all its virtual node tokens.
     */
    public RingSnapshot getRingSnapshot() {
        rwLock.readLock().lock();
        try {
            int physicalCount = physicalToTokensMap.size();
            int totalVnodes = ring.size();
            long totalSpace = hashFunction.maxHashValue() + 1L;

            Map<PhysicalNode, Integer> vnodeCounts = new LinkedHashMap<>();
            Map<PhysicalNode, Double> arcCoverage = new LinkedHashMap<>();

            if (ring.isEmpty()) {
                return new RingSnapshot(0, 0, vnodeCounts, arcCoverage, totalSpace);
            }

            for (PhysicalNode pNode : physicalToTokensMap.keySet()) {
                vnodeCounts.put(pNode, physicalToTokensMap.get(pNode).size());
            }

            // Calculate arc lengths
            List<Long> sortedTokens = new ArrayList<>(ring.keySet());
            Map<PhysicalNode, Long> nodeArcLengths = new HashMap<>();

            for (int i = 0; i < sortedTokens.size(); i++) {
                long current = sortedTokens.get(i);
                long prev;
                long arc;

                if (i == 0) {
                    prev = sortedTokens.get(sortedTokens.size() - 1);
                    // Wraparound arc from prev through maxHashValue to current
                    arc = (totalSpace - prev) + current;
                } else {
                    prev = sortedTokens.get(i - 1);
                    arc = current - prev;
                }

                VirtualNode vNode = ring.get(current);
                nodeArcLengths.merge(vNode.getPhysicalNode(), arc, Long::sum);
            }

            for (Map.Entry<PhysicalNode, Long> entry : nodeArcLengths.entrySet()) {
                double pct = (entry.getValue() * 100.0) / totalSpace;
                arcCoverage.put(entry.getKey(), pct);
            }

            return new RingSnapshot(physicalCount, totalVnodes, vnodeCounts, arcCoverage, totalSpace);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public List<PhysicalNode> getAllPhysicalNodes() {
        rwLock.readLock().lock();
        try {
            return new ArrayList<>(physicalToTokensMap.keySet());
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public int getPhysicalNodeCount() {
        rwLock.readLock().lock();
        try {
            return physicalToTokensMap.size();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public int getVirtualNodeCount() {
        rwLock.readLock().lock();
        try {
            return ring.size();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public boolean containsPhysicalNode(PhysicalNode node) {
        rwLock.readLock().lock();
        try {
            return physicalToTokensMap.containsKey(node);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public HashFunction getHashFunction() {
        return hashFunction;
    }

    public void clear() {
        rwLock.writeLock().lock();
        try {
            ring.clear();
            physicalToTokensMap.clear();
        } finally {
            rwLock.writeLock().unlock();
        }
    }
}
