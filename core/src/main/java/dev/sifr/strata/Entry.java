package dev.sifr.strata;

import java.util.Arrays;

/** A detached key/value pair returned by a scan. */
public record Entry(byte[] key, byte[] value) {
    public Entry { key = key.clone(); value = value.clone(); }
    @Override public byte[] key() { return key.clone(); }
    @Override public byte[] value() { return value.clone(); }
    @Override public boolean equals(Object other) { return other instanceof Entry e && Arrays.equals(key, e.key) && Arrays.equals(value, e.value); }
    @Override public int hashCode() { return 31 * Arrays.hashCode(key) + Arrays.hashCode(value); }
}
