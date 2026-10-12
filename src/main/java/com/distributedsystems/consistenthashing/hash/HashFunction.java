package com.distributedsystems.consistenthashing.hash;

import java.nio.charset.StandardCharsets;

/**
 * Interface defining a uniform hash function used to map keys and nodes
 * onto the consistent hashing ring.
 */
public interface HashFunction {

    /**
     * Computes a non-negative hash value for the given raw byte array.
     *
     * @param data byte array to hash
     * @return non-negative 64-bit integer token
     */
    long hash(byte[] data);

    /**
     * Computes a non-negative hash value for the given string key using UTF-8 encoding.
     *
     * @param key string key
     * @return non-negative 64-bit integer token
     */
    default long hash(String key) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        return hash(key.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Name of the hashing algorithm for metrics and logging.
     *
     * @return algorithm identifier
     */
    String algorithmName();

    /**
     * Returns the maximum possible token value for this hash function's space.
     * E.g., 2^32 - 1 for 32-bit hash space.
     */
    long maxHashValue();
}
