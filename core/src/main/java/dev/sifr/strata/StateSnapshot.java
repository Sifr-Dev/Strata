package dev.sifr.strata;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

final class StateSnapshot implements Snapshot {
    private final StorageState state;
    private final AtomicBoolean closed = new AtomicBoolean();

    StateSnapshot(StorageState state) { this.state = state; }

    @Override public Optional<byte[]> get(byte[] key) {
        ensureOpen();
        byte[] value = state.values.get(new ByteKey(key));
        return Optional.ofNullable(value == null ? null : value.clone());
    }

    @Override public Scan scan(byte[] from, byte[] to, long limit) {
        ensureOpen();
        return new StateScan(state, from, to, limit, this::ensureOpen);
    }

    @Override public void close() { closed.set(true); }
    private void ensureOpen() { if (closed.get()) throw new IllegalStateException("snapshot is closed"); }
}
