package com.distributedsystems.consistenthashing.network;

import com.distributedsystems.consistenthashing.model.PhysicalNode;
import com.distributedsystems.consistenthashing.model.VersionedValue;
import com.distributedsystems.consistenthashing.storage.NodeClient;
import com.distributedsystems.consistenthashing.storage.NodeUnavailableException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Production-ready HTTP network client communicating asynchronously with
 * remote storage nodes using non-blocking {@link java.net.http.HttpClient}.
 */
public class HttpNodeClient implements NodeClient {

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final Duration requestTimeout;

    public HttpNodeClient() {
        this(Duration.ofMillis(2000));
    }

    public HttpNodeClient(Duration requestTimeout) {
        this.httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofMillis(1000))
                .build();
        this.objectMapper = new ObjectMapper();
        this.requestTimeout = requestTimeout;
    }

    private String getBaseUrl(PhysicalNode node) {
        return "http://" + node.getHost() + ":" + node.getPort();
    }

    @Override
    public CompletableFuture<VersionedValue> get(PhysicalNode node, String key) {
        return getRaw(node, key).thenApply(val -> (val != null && !val.isTombstone()) ? val : null);
    }

    @Override
    public CompletableFuture<VersionedValue> getRaw(PhysicalNode node, String key) {
        String url = getBaseUrl(node) + "/kv/" + key;
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .GET()
                .timeout(requestTimeout)
                .build();

        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray())
                .thenApply(response -> {
                    if (response.statusCode() == 404) {
                        return null;
                    }
                    if (response.statusCode() != 200) {
                        throw new NodeUnavailableException("Node " + node.getId() + " responded with HTTP " + response.statusCode());
                    }
                    try {
                        return objectMapper.readValue(response.body(), VersionedValue.class);
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to deserialize VersionedValue from node " + node.getId(), e);
                    }
                });
    }

    @Override
    public CompletableFuture<Void> put(PhysicalNode node, String key, VersionedValue value) {
        try {
            String url = getBaseUrl(node) + "/kv/" + key;
            byte[] bodyBytes = objectMapper.writeValueAsBytes(value);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Content-Type", "application/json")
                    .PUT(HttpRequest.BodyPublishers.ofByteArray(bodyBytes))
                    .timeout(requestTimeout)
                    .build();

            return httpClient.sendAsync(request, HttpResponse.BodyHandlers.discarding())
                    .thenAccept(response -> {
                        if (response.statusCode() != 200) {
                            throw new NodeUnavailableException("Node " + node.getId() + " rejected write with HTTP " + response.statusCode());
                        }
                    });
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<Void> delete(PhysicalNode node, String key, VersionedValue tombstone) {
        try {
            String url = getBaseUrl(node) + "/kv/" + key;
            byte[] bodyBytes = objectMapper.writeValueAsBytes(tombstone);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Content-Type", "application/json")
                    .method("DELETE", HttpRequest.BodyPublishers.ofByteArray(bodyBytes))
                    .timeout(requestTimeout)
                    .build();

            return httpClient.sendAsync(request, HttpResponse.BodyHandlers.discarding())
                    .thenAccept(response -> {
                        if (response.statusCode() != 200) {
                            throw new NodeUnavailableException("Node " + node.getId() + " rejected delete with HTTP " + response.statusCode());
                        }
                    });
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<Boolean> ping(PhysicalNode node) {
        String url = getBaseUrl(node) + "/health";
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .GET()
                .timeout(Duration.ofMillis(500))
                .build();

        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.discarding())
                .thenApply(response -> response.statusCode() == 200)
                .exceptionally(ex -> false);
    }
}
