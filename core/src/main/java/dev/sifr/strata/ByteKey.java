package dev.sifr.strata;

import java.util.Arrays;

/** Unsigned lexicographic byte-string key with a cached hash. */
final class ByteKey implements Comparable<ByteKey> {
    final byte[] bytes;
    private final int hash;

    ByteKey(byte[] bytes) {
        this(bytes, true);
    }

    private ByteKey(byte[] bytes, boolean copy) {
        if (bytes.length == 0) throw new IllegalArgumentException("keys must not be empty");
        this.bytes = copy ? bytes.clone() : bytes;
        this.hash = Arrays.hashCode(this.bytes);
    }

    static ByteKey owned(byte[] bytes) { return new ByteKey(bytes, false); }

    @Override public int compareTo(ByteKey other) { return Arrays.compareUnsigned(bytes, other.bytes); }
    @Override public boolean equals(Object other) { return other instanceof ByteKey k && Arrays.equals(bytes, k.bytes); }
    @Override public int hashCode() { return hash; }
    @Override public String toString() { return java.util.HexFormat.of().formatHex(bytes); }
}
