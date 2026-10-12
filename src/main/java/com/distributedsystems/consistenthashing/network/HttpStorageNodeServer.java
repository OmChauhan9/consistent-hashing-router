package com.distributedsystems.consistenthashing.network;

import com.distributedsystems.consistenthashing.model.PhysicalNode;
import com.distributedsystems.consistenthashing.model.VersionedValue;
import com.distributedsystems.consistenthashing.storage.StorageNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;

/**
 * Lightweight, non-blocking HTTP storage node server built using standard JDK HttpServer.
 * Exposes REST endpoints for distributed cluster RPC operations.
 */
public class HttpStorageNodeServer {

    private final StorageNode storageNode;
    private final int port;
    private final HttpServer server;
    private final ObjectMapper objectMapper;

    public HttpStorageNodeServer(StorageNode storageNode, int port) throws IOException {
        this.storageNode = storageNode;
        this.port = port;
        this.objectMapper = new ObjectMapper();
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());

        this.server.createContext("/kv/", new KeyValueHandler());
        this.server.createContext("/health", new HealthHandler());
    }

    public void start() {
        server.start();
    }

    public void stop() {
        server.stop(0);
    }

    public int getPort() {
        return port;
    }

    public StorageNode getStorageNode() {
        return storageNode;
    }

    private class KeyValueHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String path = exchange.getRequestURI().getPath();
            String key = path.substring("/kv/".length());
            String method = exchange.getRequestMethod();

            try {
                if ("GET".equalsIgnoreCase(method)) {
                    VersionedValue val = storageNode.getRaw(key);
                    if (val == null || val.isTombstone()) {
                        sendResponse(exchange, 404, "{\"error\":\"Key not found\"}");
                    } else {
                        byte[] responseBytes = objectMapper.writeValueAsBytes(val);
                        exchange.getResponseHeaders().set("Content-Type", "application/json");
                        sendResponse(exchange, 200, responseBytes);
                    }
                } else if ("PUT".equalsIgnoreCase(method)) {
                    InputStream is = exchange.getRequestBody();
                    VersionedValue incoming = objectMapper.readValue(is, VersionedValue.class);
                    storageNode.put(key, incoming);
                    sendResponse(exchange, 200, "{\"status\":\"OK\"}");
                } else if ("DELETE".equalsIgnoreCase(method)) {
                    InputStream is = exchange.getRequestBody();
                    VersionedValue tombstone = objectMapper.readValue(is, VersionedValue.class);
                    storageNode.delete(key, tombstone);
                    sendResponse(exchange, 200, "{\"status\":\"DELETED\"}");
                } else {
                    sendResponse(exchange, 405, "{\"error\":\"Method not allowed\"}");
                }
            } catch (Exception e) {
                sendResponse(exchange, 500, "{\"error\":\"" + e.getMessage() + "\"}");
            }
        }
    }

    private class HealthHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            boolean healthy = storageNode.isHealthy();
            int code = healthy ? 200 : 503;
            String body = String.format("{\"id\":\"%s\",\"healthy\":%b,\"keys\":%d}",
                    storageNode.getId(), healthy, storageNode.keyCount());
            sendResponse(exchange, code, body);
        }
    }

    private void sendResponse(HttpExchange exchange, int statusCode, String responseText) throws IOException {
        sendResponse(exchange, statusCode, responseText.getBytes(StandardCharsets.UTF_8));
    }

    private void sendResponse(HttpExchange exchange, int statusCode, byte[] responseBytes) throws IOException {
        exchange.sendResponseHeaders(statusCode, responseBytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(responseBytes);
        }
    }
}
