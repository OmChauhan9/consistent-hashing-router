package com.distributedsystems.consistenthashing.ring;

import com.distributedsystems.consistenthashing.model.PhysicalNode;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Encapsulates the ordered list of distinct physical nodes responsible for replicating
 * a given key's partition, avoiding physical colocation.
 */
public class PreferenceList {

    private final String key;
    private final long keyHash;
    private final long primaryToken;
    private final List<PhysicalNode> nodes;

    public PreferenceList(String key, long keyHash, long primaryToken, List<PhysicalNode> nodes) {
        this.key = key;
        this.keyHash = keyHash;
        this.primaryToken = primaryToken;
        this.nodes = nodes != null ? List.copyOf(nodes) : Collections.emptyList();
    }

    public String getKey() {
        return key;
    }

    public long getKeyHash() {
        return keyHash;
    }

    public long getPrimaryToken() {
        return primaryToken;
    }

    /**
     * All replica nodes in clockwise preference order.
     * The first element is the primary coordinator.
     */
    public List<PhysicalNode> getNodes() {
        return nodes;
    }

    /**
     * The primary coordinator node (first physical host clockwise from the key token).
     */
    public PhysicalNode getCoordinator() {
        if (nodes.isEmpty()) {
            throw new IllegalStateException("Preference list is empty");
        }
        return nodes.get(0);
    }

    /**
     * The secondary replicas (distinct physical nodes after the coordinator).
     */
    public List<PhysicalNode> getFollowers() {
        if (nodes.size() <= 1) {
            return Collections.emptyList();
        }
        return nodes.subList(1, nodes.size());
    }

    public int size() {
        return nodes.size();
    }

    public boolean contains(PhysicalNode node) {
        return nodes.contains(node);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PreferenceList that = (PreferenceList) o;
        return keyHash == that.keyHash &&
                primaryToken == that.primaryToken &&
                Objects.equals(key, that.key) &&
                Objects.equals(nodes, that.nodes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, keyHash, primaryToken, nodes);
    }

    @Override
    public String toString() {
        return "PreferenceList{" +
                "key='" + key + '\'' +
                ", hash=" + keyHash +
                ", primaryToken=" + primaryToken +
                ", nodes=" + nodes +
                '}';
    }
}
