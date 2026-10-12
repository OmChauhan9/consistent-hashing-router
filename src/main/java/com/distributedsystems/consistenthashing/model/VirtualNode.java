package com.distributedsystems.consistenthashing.model;

import java.util.Objects;

/**
 * Represents a virtual token point on the consistent hash ring.
 * Each physical node is mapped to V virtual nodes across the ring to ensure
 * uniform distribution and prevent load hot-spots.
 */
public class VirtualNode implements Comparable<VirtualNode> {

    private final PhysicalNode physicalNode;
    private final int virtualIndex;
    private final String tokenKey;
    private final long token;

    public VirtualNode(PhysicalNode physicalNode, int virtualIndex, long token) {
        if (physicalNode == null) {
            throw new IllegalArgumentException("PhysicalNode must not be null");
        }
        if (virtualIndex < 0) {
            throw new IllegalArgumentException("VirtualIndex must be non-negative");
        }
        this.physicalNode = physicalNode;
        this.virtualIndex = virtualIndex;
        this.tokenKey = physicalNode.getId() + "#VN#" + virtualIndex;
        this.token = token;
    }

    public PhysicalNode getPhysicalNode() {
        return physicalNode;
    }

    public int getVirtualIndex() {
        return virtualIndex;
    }

    public String getTokenKey() {
        return tokenKey;
    }

    public long getToken() {
        return token;
    }

    public boolean isParent(PhysicalNode node) {
        return this.physicalNode.equals(node);
    }

    @Override
    public int compareTo(VirtualNode o) {
        int tokenCmp = Long.compare(this.token, o.token);
        if (tokenCmp != 0) {
            return tokenCmp;
        }
        return this.tokenKey.compareTo(o.tokenKey);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VirtualNode that = (VirtualNode) o;
        return token == that.token && Objects.equals(tokenKey, that.tokenKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tokenKey, token);
    }

    @Override
    public String toString() {
        return "VirtualNode{" +
                "tokenKey='" + tokenKey + '\'' +
                ", token=" + token +
                ", physicalNode=" + physicalNode.getId() +
                '}';
    }
}
