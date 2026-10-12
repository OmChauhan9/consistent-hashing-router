package com.distributedsystems.consistenthashing.hash;

/**
 * High-performance, zero-dependency implementation of MurmurHash3 (32-bit x86 variant).
 * MurmurHash3 provides excellent avalanche properties, low collision probability,
 * and extremely fast CPU execution throughput compared to cryptographic hashes.
 *
 * The 32-bit output is masked to an unsigned 64-bit long in range [0, 2^32 - 1].
 */
public class Murmur3Hash implements HashFunction {

    private static final int DEFAULT_SEED = 0x9747b28c; // Well-tested non-zero seed
    private static final long MAX_HASH_SPACE = 0xFFFFFFFFL; // 2^32 - 1

    private final int seed;

    public Murmur3Hash() {
        this(DEFAULT_SEED);
    }

    public Murmur3Hash(int seed) {
        this.seed = seed;
    }

    @Override
    public long hash(byte[] data) {
        if (data == null || data.length == 0) {
            return 0L;
        }

        int length = data.length;
        int h1 = seed;

        int c1 = 0xcc9e2d51;
        int c2 = 0x1b873593;

        // Process 4-byte chunks
        int nblocks = length / 4;
        for (int i = 0; i < nblocks; i++) {
            int index = i * 4;
            int k1 = (data[index] & 0xFF)
                    | ((data[index + 1] & 0xFF) << 8)
                    | ((data[index + 2] & 0xFF) << 16)
                    | ((data[index + 3] & 0xFF) << 24);

            k1 *= c1;
            k1 = Integer.rotateLeft(k1, 15);
            k1 *= c2;

            h1 ^= k1;
            h1 = Integer.rotateLeft(h1, 13);
            h1 = h1 * 5 + 0xe6546b64;
        }

        // Process tail bytes
        int tailIndex = nblocks * 4;
        int k1 = 0;

        switch (length & 3) {
            case 3:
                k1 ^= (data[tailIndex + 2] & 0xFF) << 16;
                // fall-through
            case 2:
                k1 ^= (data[tailIndex + 1] & 0xFF) << 8;
                // fall-through
            case 1:
                k1 ^= (data[tailIndex] & 0xFF);
                k1 *= c1;
                k1 = Integer.rotateLeft(k1, 15);
                k1 *= c2;
                h1 ^= k1;
                break;
            default:
                break;
        }

        // Finalization (avalanche mix)
        h1 ^= length;
        h1 ^= (h1 >>> 16);
        h1 *= 0x85ebca6b;
        h1 ^= (h1 >>> 13);
        h1 *= 0xc2b2ae35;
        h1 ^= (h1 >>> 16);

        // Convert to non-negative 64-bit integer representing 32-bit unsigned space [0, 2^32 - 1]
        return h1 & MAX_HASH_SPACE;
    }

    @Override
    public String algorithmName() {
        return "MurmurHash3_32bit";
    }

    @Override
    public long maxHashValue() {
        return MAX_HASH_SPACE;
    }
}
