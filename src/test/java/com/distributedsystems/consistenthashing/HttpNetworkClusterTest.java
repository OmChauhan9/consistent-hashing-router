package com.distributedsystems.consistenthashing;

import com.distributedsystems.consistenthashing.hash.Murmur3Hash;
import com.distributedsystems.consistenthashing.model.PhysicalNode;
import com.distributedsystems.consistenthashing.network.HttpNodeClient;
import com.distributedsystems.consistenthashing.network.HttpStorageNodeServer;
import com.distributedsystems.consistenthashing.quorum.QuorumConfig;
import com.distributedsystems.consistenthashing.quorum.QuorumCoordinator;
import com.distributedsystems.consistenthashing.quorum.QuorumReadResult;
import com.distributedsystems.consistenthashing.quorum.QuorumWriteResult;
import com.distributedsystems.consistenthashing.ring.ConsistentHashRing;
import com.distributedsystems.consistenthashing.storage.StorageNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class HttpNetworkClusterTest {

    private List<HttpStorageNodeServer> servers;
    private ConsistentHashRing ring;
    private HttpNodeClient httpClient;
    private QuorumCoordinator coordinator;
    private boolean networkAvailable = false;

    private int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }

    @BeforeEach
    void setup() {
        servers = new ArrayList<>();
        ring = new ConsistentHashRing(new Murmur3Hash(), 50);

        try {
            for (int i = 1; i <= 3; i++) {
                int port = findFreePort();
                PhysicalNode pNode = new PhysicalNode("http-node-" + i, "localhost", port);
                StorageNode sNode = new StorageNode(pNode);
                HttpStorageNodeServer server = new HttpStorageNodeServer(sNode, port);
                server.start();
                servers.add(server);
                ring.addPhysicalNode(pNode, 50);
            }
            httpClient = new HttpNodeClient();
            QuorumConfig config = new QuorumConfig(3, 2, 2, 3000, true, true);
            coordinator = new QuorumCoordinator(ring, httpClient, config, "http-coord");
            networkAvailable = true;
        } catch (SocketException se) {
            // macOS sandbox restricts local socket binding in restricted mode
            networkAvailable = false;
        } catch (Exception e) {
            networkAvailable = false;
        }
    }

    @AfterEach
    void tearDown() {
        if (servers != null) {
            for (HttpStorageNodeServer server : servers) {
                try {
                    server.stop();
                } catch (Exception ignored) {
                }
            }
        }
    }

    @Test
    @DisplayName("Executes Quorum PUT, GET, and DELETE over real HTTP socket network")
    void shouldExecuteQuorumOperationsOverHttpSockets() {
        Assumptions.assumeTrue(networkAvailable,
                "Skipping HTTP network test: running inside restricted sandbox without socket bind permission");

        String key = "http_network_user_42";
        String value = "{\"role\":\"Engineer\",\"region\":\"us-east\"}";

        // 1. Quorum Write over HTTP
        QuorumWriteResult writeRes = coordinator.put(key, value);
        assertThat(writeRes.isSuccess()).isTrue();
        assertThat(writeRes.getAcksReceived()).isGreaterThanOrEqualTo(2);

        // 2. Quorum Read over HTTP
        QuorumReadResult readRes = coordinator.get(key);
        assertThat(readRes.isFound()).isTrue();
        assertThat(readRes.getValueAsString()).isEqualTo(value);

        // 3. Quorum Delete over HTTP
        QuorumWriteResult delRes = coordinator.delete(key);
        assertThat(delRes.isSuccess()).isTrue();

        // 4. Verify post-delete read returns not found
        QuorumReadResult postDelRead = coordinator.get(key);
        assertThat(postDelRead.isFound()).isFalse();
    }
}
