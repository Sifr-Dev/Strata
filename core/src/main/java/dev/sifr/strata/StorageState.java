package dev.sifr.strata;

import java.util.NavigableMap;
import java.util.TreeMap;

/** Immutable published state. Mutations use copy-on-write before publication. */
final class StorageState {
    final NavigableMap<ByteKey, byte[]> values;
    final long sequence;

    StorageState(NavigableMap<ByteKey, byte[]> values, long sequence) {
        this.values = java.util.Collections.unmodifiableNavigableMap(new TreeMap<>(values));
        this.sequence = sequence;
    }

    StorageState apply(Mutation mutation) {
        TreeMap<ByteKey, byte[]> next = new TreeMap<>(values);
        ByteKey key = new ByteKey(mutation.key());
        if (mutation.type() == Mutation.Type.PUT) next.put(key, mutation.value());
        else next.remove(key);
        return new StorageState(next, sequence + 1);
    }

    long estimatedBytes() {
        long total = 0;
        for (var entry : values.entrySet()) {
            total = Math.addExact(total, Math.addExact(entry.getKey().bytes.length, entry.getValue().length));
        }
        return total;
    }
}
