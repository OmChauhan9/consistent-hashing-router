package com.distributedsystems.consistenthashing.quorum;

import com.distributedsystems.consistenthashing.model.PhysicalNode;
import com.distributedsystems.consistenthashing.model.VersionedValue;
import com.distributedsystems.consistenthashing.ring.ConsistentHashRing;
import com.distributedsystems.consistenthashing.ring.PreferenceList;
import com.distributedsystems.consistenthashing.storage.NodeClient;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Quorum Coordinator executing asynchronous parallel reads, writes, and deletes
 * across a key's replication preference list.
 *
 * <p>Enforces tunable consistency levels (N, W, R), performs conflict reconciliation
 * via Vector Clocks / Last-Write-Wins, triggers asynchronous Read Repair on stale replicas,
 * and supports Sloppy Quorum routing.</p>
 */
public class QuorumCoordinator {

    private final ConsistentHashRing ring;
    private final NodeClient nodeClient;
    private final QuorumConfig config;
    private final String coordinatorId;

    public QuorumCoordinator(ConsistentHashRing ring, NodeClient nodeClient) {
        this(ring, nodeClient, QuorumConfig.defaultDynamo(), "coordinator-default");
    }

    public QuorumCoordinator(ConsistentHashRing ring, NodeClient nodeClient, QuorumConfig config) {
        this(ring, nodeClient, config, "coordinator-" + System.currentTimeMillis());
    }

    public QuorumCoordinator(ConsistentHashRing ring, NodeClient nodeClient, QuorumConfig config, String coordinatorId) {
        if (ring == null) throw new IllegalArgumentException("Ring must not be null");
        if (nodeClient == null) throw new IllegalArgumentException("NodeClient must not be null");
        if (config == null) throw new IllegalArgumentException("QuorumConfig must not be null");
        this.ring = ring;
        this.nodeClient = nodeClient;
        this.config = config;
        this.coordinatorId = coordinatorId != null ? coordinatorId : "coordinator";
    }

    /**
     * Executes a Quorum Write for the given key and string value.
     */
    public QuorumWriteResult put(String key, String value) {
        byte[] bytes = value != null ? value.getBytes(StandardCharsets.UTF_8) : new byte[0];
        return put(key, bytes);
    }

    /**
     * Executes a Quorum Write for the given key and raw bytes payload.
     * Dispatches async writes in parallel to the key's preference list of N physical nodes.
     * Returns successfully once W acknowledgments are collected.
     */
    public QuorumWriteResult put(String key, byte[] value) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        long startTime = System.nanoTime();
        PreferenceList prefList = ring.getPreferenceList(key, config.getReplicationFactor());
        if (prefList.size() == 0) {
            throw new IllegalStateException("Cannot execute write: Hash ring contains no registered nodes");
        }

        List<PhysicalNode> targetNodes = resolveTargetNodes(prefList);
        VersionedValue versionedValue = VersionedValue.of(new String(value, StandardCharsets.UTF_8), coordinatorId);

        int writeQuorum = Math.min(config.getWriteQuorum(), targetNodes.size());
        CountDownLatch latch = new CountDownLatch(writeQuorum);
        AtomicInteger acks = new AtomicInteger(0);
        List<PhysicalNode> acknowledgedNodes = new CopyOnWriteArrayList<>();
        List<PhysicalNode> failedNodes = new CopyOnWriteArrayList<>();

        for (PhysicalNode node : targetNodes) {
            nodeClient.put(node, key, versionedValue)
                    .whenComplete((res, ex) -> {
                        if (ex == null) {
                            acks.incrementAndGet();
                            acknowledgedNodes.add(node);
                            latch.countDown();
                        } else {
                            failedNodes.add(node);
                        }
                    });
        }

        boolean reachedQuorum;
        try {
            reachedQuorum = latch.await(config.getTimeoutMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new QuorumException(key, writeQuorum, acks.get(), "Write operation was interrupted", e);
        }

        long latency = System.nanoTime() - startTime;
        int totalAcks = acks.get();

        if (!reachedQuorum && totalAcks < writeQuorum) {
            throw new QuorumException(key, writeQuorum, totalAcks,
                    "Write quorum timed out. Acked: " + totalAcks + ", Failed: " + failedNodes.size());
        }

        return new QuorumWriteResult(
                key,
                true,
                totalAcks,
                writeQuorum,
                prefList,
                acknowledgedNodes,
                failedNodes,
                latency
        );
    }

    /**
     * Executes a Quorum Read for the given key.
     * Dispatches async reads in parallel to the key's preference list of N physical nodes.
     * Gathers responses until R responses are collected, performs causal reconciliation,
     * and triggers asynchronous Read Repair on stale replicas.
     */
    public QuorumReadResult get(String key) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        long startTime = System.nanoTime();
        PreferenceList prefList = ring.getPreferenceList(key, config.getReplicationFactor());
        if (prefList.size() == 0) {
            return new QuorumReadResult(key, null, false, 0, config.getReadQuorum(), prefList, false, Collections.emptyList(), System.nanoTime() - startTime);
        }

        List<PhysicalNode> targetNodes = resolveTargetNodes(prefList);
        int readQuorum = Math.min(config.getReadQuorum(), targetNodes.size());

        CountDownLatch latch = new CountDownLatch(readQuorum);
        AtomicInteger responseCount = new AtomicInteger(0);
        List<NodeResponse> responses = new CopyOnWriteArrayList<>();
        List<CompletableFuture<Void>> allFutures = new ArrayList<>();

        for (PhysicalNode node : targetNodes) {
            CompletableFuture<Void> future = nodeClient.getRaw(node, key)
                    .thenAccept(val -> {
                        responses.add(new NodeResponse(node, val));
                        responseCount.incrementAndGet();
                        latch.countDown();
                    })
                    .exceptionally(ex -> null);
            allFutures.add(future);
        }

        boolean reachedQuorum;
        try {
            reachedQuorum = latch.await(config.getTimeoutMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new QuorumException(key, readQuorum, responseCount.get(), "Read operation was interrupted", e);
        }

        int totalResponses = responseCount.get();
        if (!reachedQuorum && totalResponses < readQuorum) {
            throw new QuorumException(key, readQuorum, totalResponses, "Read quorum timed out");
        }

        // Reconcile highest / causal version across currently received responses
        VersionedValue winningVersion = null;
        for (NodeResponse resp : responses) {
            winningVersion = VersionedValue.reconcile(winningVersion, resp.value);
        }

        // Background Read Repair (Cassandra-style): wait briefly or asynchronously inspect all N replicas
        boolean repairTriggered = false;
        List<PhysicalNode> repairedNodes = new CopyOnWriteArrayList<>();

        if (config.isReadRepairEnabled() && winningVersion != null) {
            // First check currently collected responses
            for (NodeResponse resp : responses) {
                if (isStale(resp.value, winningVersion)) {
                    repairTriggered = true;
                    repairedNodes.add(resp.node);
                    nodeClient.put(resp.node, key, winningVersion);
                }
            }

            // Asynchronously attach to all remaining replicas to heal any late-arriving stale replica
            final VersionedValue finalWinner = winningVersion;
            CompletableFuture.allOf(allFutures.toArray(new CompletableFuture[0]))
                    .thenRun(() -> {
                        for (NodeResponse resp : responses) {
                            if (isStale(resp.value, finalWinner) && !repairedNodes.contains(resp.node)) {
                                repairedNodes.add(resp.node);
                                nodeClient.put(resp.node, key, finalWinner);
                            }
                        }
                    });
        }

        long latency = System.nanoTime() - startTime;
        boolean found = winningVersion != null && !winningVersion.isTombstone();

        return new QuorumReadResult(
                key,
                winningVersion,
                found,
                totalResponses,
                readQuorum,
                prefList,
                repairTriggered,
                repairedNodes,
                latency
        );
    }

    /**
     * Executes a Quorum Delete by writing a tombstone version across the preference list.
     */
    public QuorumWriteResult delete(String key) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        long startTime = System.nanoTime();
        PreferenceList prefList = ring.getPreferenceList(key, config.getReplicationFactor());
        if (prefList.size() == 0) {
            throw new IllegalStateException("Cannot execute delete: Hash ring contains no registered nodes");
        }

        List<PhysicalNode> targetNodes = resolveTargetNodes(prefList);
        VersionedValue tombstone = VersionedValue.tombstone(coordinatorId);

        int writeQuorum = Math.min(config.getWriteQuorum(), targetNodes.size());
        CountDownLatch latch = new CountDownLatch(writeQuorum);
        AtomicInteger acks = new AtomicInteger(0);
        List<PhysicalNode> acknowledgedNodes = new CopyOnWriteArrayList<>();
        List<PhysicalNode> failedNodes = new CopyOnWriteArrayList<>();

        for (PhysicalNode node : targetNodes) {
            nodeClient.delete(node, key, tombstone)
                    .whenComplete((res, ex) -> {
                        if (ex == null) {
                            acks.incrementAndGet();
                            acknowledgedNodes.add(node);
                            latch.countDown();
                        } else {
                            failedNodes.add(node);
                        }
                    });
        }

        boolean reachedQuorum;
        try {
            reachedQuorum = latch.await(config.getTimeoutMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new QuorumException(key, writeQuorum, acks.get(), "Delete operation was interrupted", e);
        }

        long latency = System.nanoTime() - startTime;
        int totalAcks = acks.get();

        if (!reachedQuorum && totalAcks < writeQuorum) {
            throw new QuorumException(key, writeQuorum, totalAcks, "Delete quorum timed out");
        }

        return new QuorumWriteResult(
                key,
                true,
                totalAcks,
                writeQuorum,
                prefList,
                acknowledgedNodes,
                failedNodes,
                latency
        );
    }

    private List<PhysicalNode> resolveTargetNodes(PreferenceList prefList) {
        List<PhysicalNode> nodes = prefList.getNodes();
        if (!config.isSloppyQuorumEnabled()) {
            return nodes;
        }

        // If sloppy quorum enabled, ensure we attempt healthy nodes first
        List<PhysicalNode> healthy = new ArrayList<>();
        List<PhysicalNode> dead = new ArrayList<>();
        for (PhysicalNode n : nodes) {
            if (n.isAvailable()) {
                healthy.add(n);
            } else {
                dead.add(n);
            }
        }
        // If some primary nodes are dead, sloppy quorum appends remaining nodes
        healthy.addAll(dead);
        return healthy;
    }

    private boolean isStale(VersionedValue candidate, VersionedValue winning) {
        if (candidate == null) return true;
        if (Objects.equals(candidate, winning)) return false;
        return candidate.getTimestamp() < winning.getTimestamp();
    }

    public ConsistentHashRing getRing() {
        return ring;
    }

    public QuorumConfig getConfig() {
        return config;
    }

    public String getCoordinatorId() {
        return coordinatorId;
    }

    private static class NodeResponse {
        final PhysicalNode node;
        final VersionedValue value;

        NodeResponse(PhysicalNode node, VersionedValue value) {
            this.node = node;
            this.value = value;
        }
    }
}
