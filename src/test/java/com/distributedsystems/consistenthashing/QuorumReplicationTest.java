package com.distributedsystems.consistenthashing;

import com.distributedsystems.consistenthashing.hash.Murmur3Hash;
import com.distributedsystems.consistenthashing.model.PhysicalNode;
import com.distributedsystems.consistenthashing.model.VersionedValue;
import com.distributedsystems.consistenthashing.quorum.QuorumConfig;
import com.distributedsystems.consistenthashing.quorum.QuorumCoordinator;
import com.distributedsystems.consistenthashing.quorum.QuorumException;
import com.distributedsystems.consistenthashing.quorum.QuorumReadResult;
import com.distributedsystems.consistenthashing.quorum.QuorumWriteResult;
import com.distributedsystems.consistenthashing.ring.ConsistentHashRing;
import com.distributedsystems.consistenthashing.storage.LocalNodeClient;
import com.distributedsystems.consistenthashing.storage.StorageNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class QuorumReplicationTest {

    private ConsistentHashRing ring;
    private LocalNodeClient transport;
    private List<StorageNode> nodes;
    private QuorumCoordinator coordinator;

    @BeforeEach
    void setup() {
        ring = new ConsistentHashRing(new Murmur3Hash(), 100);
        transport = new LocalNodeClient();
        nodes = new ArrayList<>();

        for (int i = 1; i <= 5; i++) {
            PhysicalNode pNode = new PhysicalNode("node-0" + i);
            StorageNode sNode = new StorageNode(pNode);
            nodes.add(sNode);
            transport.registerNode(sNode);
            ring.addPhysicalNode(pNode, 100);
        }

        QuorumConfig config = new QuorumConfig(3, 2, 2); // N=3, W=2, R=2
        coordinator = new QuorumCoordinator(ring, transport, config, "test-coord");
    }

    @Test
    @DisplayName("Quorum write succeeds with W=2 acknowledgments across N=3 replicas")
    void shouldExecuteQuorumWriteSuccessfully() {
        String key = "test_user_key";
        String val = "payload_data_v1";

        QuorumWriteResult writeRes = coordinator.put(key, val);
        assertThat(writeRes.isSuccess()).isTrue();
        assertThat(writeRes.getAcksReceived()).isGreaterThanOrEqualTo(2);
        assertThat(writeRes.getPreferenceList().size()).isEqualTo(3);

        // Verify key exists on the acknowledged storage nodes
        for (PhysicalNode ackedNode : writeRes.getAcknowledgedNodes()) {
            StorageNode node = transport.getNode(ackedNode.getId());
            assertThat(node.get(key)).isNotNull();
            assertThat(node.get(key).getValueAsString()).isEqualTo(val);
        }
    }

    @Test
    @DisplayName("Quorum read succeeds with R=2 responses")
    void shouldExecuteQuorumReadSuccessfully() {
        String key = "test_user_read";
        String val = "payload_data_read";

        coordinator.put(key, val);
        QuorumReadResult readRes = coordinator.get(key);

        assertThat(readRes.isFound()).isTrue();
        assertThat(readRes.getValueAsString()).isEqualTo(val);
        assertThat(readRes.getResponsesReceived()).isGreaterThanOrEqualTo(2);
    }

    @Test
    @DisplayName("Tolerates single node crash when N=3, W=2, R=2")
    void shouldTolerateSingleNodeFailure() {
        String key = "fault_tolerance_key";
        String val = "healthy_data";

        coordinator.put(key, val);
        QuorumReadResult initialRead = coordinator.get(key);
        PhysicalNode coordinatorReplica = initialRead.getPreferenceList().getCoordinator();

        // Crash the primary coordinator replica
        StorageNode crashedNode = transport.getNode(coordinatorReplica.getId());
        crashedNode.simulateCrash();

        // Read must still succeed via the other 2 surviving replicas in preference list
        QuorumReadResult readAfterCrash = coordinator.get(key);
        assertThat(readAfterCrash.isFound()).isTrue();
        assertThat(readAfterCrash.getValueAsString()).isEqualTo(val);

        // Write must also succeed because W=2 can be satisfied by remaining 2 replicas
        QuorumWriteResult writeAfterCrash = coordinator.put(key, "updated_during_failure");
        assertThat(writeAfterCrash.isSuccess()).isTrue();
    }

    @Test
    @DisplayName("Fails write quorum when 2 of 3 replicas crash (W=2 cannot be satisfied)")
    void shouldFailWhenQuorumCannotBeSatisfied() {
        String key = "strict_failure_key";
        QuorumConfig strictConfig = new QuorumConfig(3, 2, 2, 500, false, false);
        QuorumCoordinator strictCoord = new QuorumCoordinator(ring, transport, strictConfig);

        List<PhysicalNode> prefNodes = ring.getPreferenceList(key, 3).getNodes();

        // Crash 2 out of 3 replicas
        transport.getNode(prefNodes.get(0).getId()).simulateCrash();
        transport.getNode(prefNodes.get(1).getId()).simulateCrash();

        assertThatThrownBy(() -> strictCoord.put(key, "data"))
                .isInstanceOf(QuorumException.class)
                .hasMessageContaining("Write quorum timed out");
    }

    @Test
    @DisplayName("Read Repair heals stale replicas asynchronously upon detecting version divergence")
    void shouldPerformReadRepairOnStaleReplica() throws InterruptedException {
        String key = "read_repair_test_key";
        String winningValue = "latest_version_v2";

        coordinator.put(key, winningValue);
        QuorumReadResult read = coordinator.get(key);
        List<PhysicalNode> replicas = read.getPreferenceList().getNodes();

        PhysicalNode staleNodeTarget = replicas.get(1);
        StorageNode staleStorage = transport.getNode(staleNodeTarget.getId());

        // Clear existing entry to simulate a node having missed the write or holding an old snapshot
        staleStorage.getStore().clear();

        // Populate stale node with older version and older timestamp
        VersionedValue staleVersion = new VersionedValue(
                "stale_version_v1".getBytes(),
                System.currentTimeMillis() - 20_000,
                null,
                false
        );
        staleStorage.getStore().put(key, staleVersion);
        assertThat(staleStorage.get(key).getValueAsString()).isEqualTo("stale_version_v1");

        // Execute Quorum Read: should detect divergence and trigger Read Repair
        QuorumReadResult repairedRead = coordinator.get(key);
        assertThat(repairedRead.getValueAsString()).isEqualTo(winningValue);
        assertThat(repairedRead.isReadRepairTriggered()).isTrue();
        assertThat(repairedRead.getRepairedNodes()).contains(staleNodeTarget);

        // Allow async read repair thread to complete
        Thread.sleep(150);

        // Verify stale replica was healed with the latest winning value!
        assertThat(staleStorage.get(key).getValueAsString()).isEqualTo(winningValue);
    }

    @Test
    @DisplayName("Quorum Delete writes tombstone and subsequent read returns not found")
    void shouldDeleteWithTombstone() {
        String key = "delete_tombstone_key";
        coordinator.put(key, "to_be_deleted");

        QuorumReadResult beforeDelete = coordinator.get(key);
        assertThat(beforeDelete.isFound()).isTrue();

        QuorumWriteResult deleteRes = coordinator.delete(key);
        assertThat(deleteRes.isSuccess()).isTrue();

        QuorumReadResult afterDelete = coordinator.get(key);
        assertThat(afterDelete.isFound()).isFalse();
    }
}
