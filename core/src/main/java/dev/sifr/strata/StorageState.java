package dev.sifr.strata;

import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * An immutable point-in-time view of the sorted state. Construction takes
 * ownership of a map that the caller copied under the writer lock; mutation
 * methods exist only for recovery-time replay of a private map.
 */
final class StorageState {
    final NavigableMap<ByteKey, byte[]> values;
    final long sequence;

    StorageState(NavigableMap<ByteKey, byte[]> owned, long sequence) {
        this.values = java.util.Collections.unmodifiableNavigableMap(owned);
        this.sequence = sequence;
    }

    /** Replay helper used while recovering a private, not yet published map. */
    static NavigableMap<ByteKey, byte[]> applyTo(NavigableMap<ByteKey, byte[]> target, Mutation mutation) {
        ByteKey key = new ByteKey(mutation.key());
        if (mutation.type() == Mutation.Type.PUT) target.put(key, mutation.value());
        else target.remove(key);
        return target;
    }

    long estimatedBytes() {
        long total = 0;
        for (var entry : values.entrySet()) {
            total = Math.addExact(total, Math.addExact(entry.getKey().bytes.length, entry.getValue().length));
        }
        return total;
    }
}
