package com.distributedsystems.consistenthashing.model;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a physical hardware server or container instance in the distributed cluster.
 * Supports configurable capacity weighting for heterogeneous hardware clusters.
 */
public class PhysicalNode {

    private final String id;
    private final String host;
    private final int port;
    private final double weight;
    private volatile NodeStatus status;
    private final Map<String, String> metadata;

    public PhysicalNode(String id) {
        this(id, "localhost", 8080, 1.0, NodeStatus.HEALTHY, Collections.emptyMap());
    }

    public PhysicalNode(String id, String host, int port) {
        this(id, host, port, 1.0, NodeStatus.HEALTHY, Collections.emptyMap());
    }

    public PhysicalNode(String id, String host, int port, double weight) {
        this(id, host, port, weight, NodeStatus.HEALTHY, Collections.emptyMap());
    }

    public PhysicalNode(String id, String host, int port, double weight, NodeStatus status, Map<String, String> metadata) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("PhysicalNode id must not be null or blank");
        }
        if (weight <= 0.0) {
            throw new IllegalArgumentException("PhysicalNode weight must be strictly positive");
        }
        this.id = id;
        this.host = host != null ? host : "localhost";
        this.port = port;
        this.weight = weight;
        this.status = status != null ? status : NodeStatus.HEALTHY;
        this.metadata = metadata != null ? new HashMap<>(metadata) : new HashMap<>();
    }

    public String getId() {
        return id;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public double getWeight() {
        return weight;
    }

    public NodeStatus getStatus() {
        return status;
    }

    public void setStatus(NodeStatus status) {
        this.status = status;
    }

    public boolean isAvailable() {
        return status == NodeStatus.HEALTHY;
    }

    public Map<String, String> getMetadata() {
        return Collections.unmodifiableMap(metadata);
    }

    public String getAddress() {
        return host + ":" + port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PhysicalNode that = (PhysicalNode) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "PhysicalNode{" +
                "id='" + id + '\'' +
                ", addr='" + getAddress() + '\'' +
                ", weight=" + weight +
                ", status=" + status +
                '}';
    }
}
