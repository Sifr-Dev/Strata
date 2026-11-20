package dev.sifr.strata;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/** Cursor over immutable sorted state; detached values prevent ownership leaks. */
final class StateScan implements Scan {
    private final Iterator<Map.Entry<ByteKey, byte[]>> iterator;
    private final long limit;
    private final Runnable ensureOpen;
    private final AtomicBoolean closed = new AtomicBoolean();
    private long count;
    private Entry current;

    StateScan(StorageState state, byte[] from, byte[] to, long limit, Runnable ensureOpen) {
        if (from == null || to == null || from.length == 0 || to.length == 0) throw new IllegalArgumentException("scan bounds must be non-empty");
        if (limit < 0) throw new IllegalArgumentException("scan limit must be non-negative");
        ByteKey start = new ByteKey(from), end = new ByteKey(to);
        if (start.compareTo(end) >= 0) throw new IllegalArgumentException("scan lower bound must be less than upper bound");
        iterator = state.values.subMap(start, true, end, false).entrySet().iterator();
        this.limit = limit;
        this.ensureOpen = ensureOpen;
    }

    @Override public boolean next() {
        if (closed.get()) throw new IllegalStateException("scan is closed");
        ensureOpen.run();
        if (count >= limit || !iterator.hasNext()) { current = null; return false; }
        var next = iterator.next();
        current = new Entry(next.getKey().bytes, next.getValue());
        count++;
        return true;
    }

    @Override public Entry entry() {
        if (current == null) throw new IllegalStateException("next() must return true before entry()");
        return current;
    }

    @Override public void close() { closed.set(true); current = null; }
}
