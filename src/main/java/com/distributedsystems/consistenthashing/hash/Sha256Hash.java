package com.distributedsystems.consistenthashing.hash;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Cryptographic SHA-256 hash function mapping keys onto the consistent hash ring.
 * While computationally heavier than MurmurHash3, SHA-256 guarantees uniform bit entropy
 * and resistance to preimage attacks.
 */
public class Sha256Hash implements HashFunction {

    private static final long MAX_HASH_SPACE = 0xFFFFFFFFL;

    @Override
    public long hash(byte[] data) {
        if (data == null || data.length == 0) {
            return 0L;
        }

        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(data);

            // Extract the first 4 bytes as an unsigned 32-bit integer in range [0, 2^32 - 1]
            long h = ((digest[0] & 0xFFL) << 24)
                    | ((digest[1] & 0xFFL) << 16)
                    | ((digest[2] & 0xFFL) << 8)
                    | (digest[3] & 0xFFL);

            return h & MAX_HASH_SPACE;
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 algorithm unavailable in JVM", e);
        }
    }

    @Override
    public String algorithmName() {
        return "SHA-256";
    }

    @Override
    public long maxHashValue() {
        return MAX_HASH_SPACE;
    }
}
