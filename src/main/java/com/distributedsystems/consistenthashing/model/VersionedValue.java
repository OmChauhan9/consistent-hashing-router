package com.distributedsystems.consistenthashing.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/**
 * Value container stored inside replicas, encapsulating raw payload,
 * physical wall-clock timestamp (for Last-Write-Wins fallback),
 * vector clock (for causality detection), and tombstone flag.
 */
public class VersionedValue {

    private final byte[] value;
    private final long timestamp;
    private final VectorClock vectorClock;
    private final boolean tombstone;

    public static VersionedValue of(String value, String coordinatorId) {
        byte[] bytes = value != null ? value.getBytes(StandardCharsets.UTF_8) : new byte[0];
        long now = System.currentTimeMillis();
        VectorClock vc = new VectorClock().increment(coordinatorId);
        return new VersionedValue(bytes, now, vc, false);
    }

    public static VersionedValue tombstone(String coordinatorId) {
        long now = System.currentTimeMillis();
        VectorClock vc = new VectorClock().increment(coordinatorId);
        return new VersionedValue(new byte[0], now, vc, true);
    }

    @JsonCreator
    public VersionedValue(
            @JsonProperty("value") byte[] value,
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("vectorClock") VectorClock vectorClock,
            @JsonProperty("tombstone") boolean tombstone) {
        this.value = value != null ? value.clone() : new byte[0];
        this.timestamp = timestamp;
        this.vectorClock = vectorClock != null ? vectorClock : new VectorClock();
        this.tombstone = tombstone;
    }

    public byte[] getValue() {
        return value.clone();
    }

    public String getValueAsString() {
        return new String(value, StandardCharsets.UTF_8);
    }

    public long getTimestamp() {
        return timestamp;
    }

    public VectorClock getVectorClock() {
        return vectorClock;
    }

    public boolean isTombstone() {
        return tombstone;
    }

    /**
     * Creates an updated VersionedValue causally succeeding this one.
     */
    public VersionedValue update(byte[] newValue, String coordinatorId) {
        long now = Math.max(System.currentTimeMillis(), this.timestamp + 1);
        VectorClock nextClock = this.vectorClock.increment(coordinatorId);
        return new VersionedValue(newValue, now, nextClock, false);
    }

    /**
     * Resolves between two replicas using Vector Clocks first,
     * falling back to Last-Write-Wins (LWW) timestamp on concurrency.
     */
    public static VersionedValue reconcile(VersionedValue v1, VersionedValue v2) {
        if (v1 == null) return v2;
        if (v2 == null) return v1;

        VectorClock.Ordering ordering = v1.vectorClock.compare(v2.vectorClock);
        if (ordering == VectorClock.Ordering.AFTER) {
            return v1;
        } else if (ordering == VectorClock.Ordering.BEFORE) {
            return v2;
        }

        // Concurrent divergence: fallback to LWW (highest timestamp)
        if (v1.timestamp > v2.timestamp) {
            return new VersionedValue(v1.value, v1.timestamp, v1.vectorClock.merge(v2.vectorClock), v1.tombstone);
        } else {
            return new VersionedValue(v2.value, v2.timestamp, v1.vectorClock.merge(v2.vectorClock), v2.tombstone);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VersionedValue that = (VersionedValue) o;
        return timestamp == that.timestamp &&
                tombstone == that.tombstone &&
                Arrays.equals(value, that.value) &&
                Objects.equals(vectorClock, that.vectorClock);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(timestamp, vectorClock, tombstone);
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }

    @Override
    public String toString() {
        return "VersionedValue{" +
                "value='" + (tombstone ? "<TOMBSTONE>" : getValueAsString()) + '\'' +
                ", ts=" + timestamp +
                ", clock=" + vectorClock +
                ", tombstone=" + tombstone +
                '}';
    }
}
