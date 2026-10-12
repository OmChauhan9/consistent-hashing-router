package com.distributedsystems.consistenthashing.model;

/**
 * Health and operational state of a physical node in the distributed cluster.
 */
public enum NodeStatus {
    /** Node is fully operational and accepting reads and writes. */
    HEALTHY,

    /** Node failed heartbeat or missed ACKs; temporarily bypassed or fallback target. */
    SUSPECT,

    /** Node is marked offline or dead; bypassed during preference list routing. */
    DEAD
}
