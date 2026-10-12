package com.distributedsystems.consistenthashing.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Distributed Vector Clock capturing causality and detecting concurrent updates across replicas.
 * Each entry tracks the monotonic write counter seen by a specific coordinator node.
 */
public class VectorClock {

    public enum Ordering {
        EQUAL,
        BEFORE,
        AFTER,
        CONCURRENT
    }

    private final Map<String, Long> versions;

    public VectorClock() {
        this.versions = new HashMap<>();
    }

    @JsonCreator
    public VectorClock(@JsonProperty("versions") Map<String, Long> versions) {
        this.versions = versions != null ? new HashMap<>(versions) : new HashMap<>();
    }

    /**
     * Creates a new VectorClock with the given node's counter incremented by 1.
     */
    public VectorClock increment(String nodeId) {
        if (nodeId == null || nodeId.isBlank()) {
            throw new IllegalArgumentException("NodeId cannot be null or blank");
        }
        Map<String, Long> copy = new HashMap<>(this.versions);
        long current = copy.getOrDefault(nodeId, 0L);
        copy.put(nodeId, current + 1);
        return new VectorClock(copy);
    }

    /**
     * Compares causality with another VectorClock.
     *
     * @param other vector clock to compare
     * @return EQUAL, BEFORE (ancestor), AFTER (descendant), or CONCURRENT (divergent write)
     */
    public Ordering compare(VectorClock other) {
        if (other == null) {
            return Ordering.AFTER;
        }

        boolean thisHasGreater = false;
        boolean otherHasGreater = false;

        Set<String> allNodes = new HashSet<>(this.versions.keySet());
        allNodes.addAll(other.versions.keySet());

        for (String node : allNodes) {
            long thisVal = this.versions.getOrDefault(node, 0L);
            long otherVal = other.versions.getOrDefault(node, 0L);

            if (thisVal > otherVal) {
                thisHasGreater = true;
            } else if (otherVal > thisVal) {
                otherHasGreater = true;
            }
        }

        if (thisHasGreater && otherHasGreater) {
            return Ordering.CONCURRENT;
        }
        if (thisHasGreater) {
            return Ordering.AFTER;
        }
        if (otherHasGreater) {
            return Ordering.BEFORE;
        }
        return Ordering.EQUAL;
    }

    /**
     * Merges this vector clock with another by taking the maximum clock value for each node.
     */
    public VectorClock merge(VectorClock other) {
        if (other == null) {
            return new VectorClock(this.versions);
        }
        Map<String, Long> merged = new HashMap<>(this.versions);
        for (Map.Entry<String, Long> entry : other.versions.entrySet()) {
            merged.merge(entry.getKey(), entry.getValue(), Math::max);
        }
        return new VectorClock(merged);
    }

    public Map<String, Long> getVersions() {
        return Collections.unmodifiableMap(versions);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VectorClock that = (VectorClock) o;
        return Objects.equals(versions, that.versions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(versions);
    }

    @Override
    public String toString() {
        return versions.toString();
    }
}
