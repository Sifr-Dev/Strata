package dev.sifr.strata;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

/** Single process, serialized writer, copy-on-write readers. */
final class StrataEngine implements Strata {
    private final StrataOptions options;
    private final Path directory;
    private final Path logPath;
    private final Path segmentPath;
    private final Path manifestPath;
    private final ReentrantLock writer = new ReentrantLock();
    private final Object lifecycle = new Object();
    private FileChannel log;
    private FileChannel lease;
    private java.nio.channels.FileLock leaseLock;
    private volatile StorageState state;
    private long generation;
    private long liveBytes;
    private boolean closed;
    private IOException failure;

    private StrataEngine(StrataOptions options) {
        this.options = options;
        directory = options.directory().toAbsolutePath().normalize();
        logPath = directory.resolve("wal.log");
        segmentPath = directory.resolve("segment-1.sst");
        manifestPath = directory.resolve("MANIFEST");
    }

    static StrataEngine open(StrataOptions options) throws IOException {
        StrataEngine engine = new StrataEngine(options);
        engine.recover();
        return engine;
    }

    private void recover() throws IOException {
        Files.createDirectories(directory);
        // Retain both the channel and lock for the store's complete lifetime.
        lease = FileChannel.open(directory.resolve("LOCK"), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        try {
            leaseLock = lease.tryLock();
            if (leaseLock == null) throw new IOException("database is already opened by another process");
        } catch (java.nio.channels.OverlappingFileLockException error) {
            lease.close();
            throw new IOException("database is already opened in this process", error);
        } catch (Throwable error) {
            lease.close();
            throw error;
        }
        long[] manifest = Format.readManifest(manifestPath);
        NavigableMap<ByteKey, byte[]> recovered = Format.readSegment(segmentPath, options);
        StorageState next = new StorageState(recovered, manifest[0]);
        generation = manifest[1];
        for (Mutation mutation : Format.readLog(logPath)) {
            Format.validate(mutation.key(), mutation.value(), options);
            next = next.apply(mutation);
        }
        state = next;
        liveBytes = next.estimatedBytes();
        // Any replay (including a truncated final frame) is checkpointed into a fresh WAL.
        // Never append after an incomplete record, and do not reapply an already checkpointed log.
        checkpoint(next);
        // Explicit close owns resource lifetime; no per-open JVM shutdown hooks are accumulated.
    }

    @Override public Optional<byte[]> get(byte[] key) throws IOException {
        Objects.requireNonNull(key, "key");
        checkReadable();
        byte[] value = state.values.get(new ByteKey(key));
        return Optional.ofNullable(value == null ? null : value.clone());
    }

    @Override public void put(byte[] key, byte[] value) throws IOException { write(List.of(Mutation.put(key, value))); }
    @Override public void delete(byte[] key) throws IOException { write(List.of(Mutation.delete(key))); }

    @Override public void write(Iterable<Mutation> mutations) throws IOException {
        Objects.requireNonNull(mutations, "mutations");
        writer.lock();
        try {
            checkWritable();
            List<Mutation> batch = new ArrayList<>();
            StorageState next = state;
            long nextBytes = liveBytes;
            for (Mutation input : mutations) {
                Objects.requireNonNull(input, "mutation");
                Mutation mutation = new Mutation(input.type(), input.key(), input.value());
                Format.validate(mutation.key(), mutation.value(), options);
                ByteKey key = new ByteKey(mutation.key());
                byte[] old = next.values.get(key);
                if (old != null) nextBytes -= key.bytes.length + old.length;
                if (mutation.type() == Mutation.Type.PUT) nextBytes += mutation.key().length + mutation.value().length;
                next = next.apply(mutation);
                batch.add(mutation);
            }
            if (batch.isEmpty()) return;
            if (nextBytes > options.maxLiveBytes()) throw new IOException("live-byte limit reached; compacting cannot reclaim live values");
            try {
                Format.writeFully(log, ByteBuffer.wrap(Format.frame(batch)));
                if (options.durability() == StrataOptions.Durability.SYNC) log.force(false);
            } catch (IOException error) {
                fail(error);
                throw error;
            }
            state = next;
            liveBytes = nextBytes;
            if (log.size() >= options.maxSegmentBytes()) compactLocked();
        } finally { writer.unlock(); }
    }

    @Override public Snapshot snapshot() { return new StateSnapshot(state); }

    @Override public Scan scan(byte[] from, byte[] to, long limit) {
        checkReadableUnchecked();
        return new StateScan(state, from, to, limit, this::checkReadableUnchecked);
    }

    @Override public void compact() throws IOException {
        writer.lock();
        try { checkWritable(); compactLocked(); }
        finally { writer.unlock(); }
    }

    private void compactLocked() throws IOException {
        checkpoint(state);
    }

    private void checkpoint(StorageState checkpoint) throws IOException {
        FileChannel previousLog = log;
        try {
            long nextGeneration = generation + 1;
            Path segmentTmp = directory.resolve("segment.tmp");
            Path manifestTmp = directory.resolve("MANIFEST.tmp");
            Files.deleteIfExists(segmentTmp);
            Files.deleteIfExists(manifestTmp);
            Format.writeSegment(segmentTmp, checkpoint.values);
            Format.writeManifest(manifestTmp, checkpoint.sequence, nextGeneration);
            try {
                Files.move(segmentTmp, segmentPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                Files.move(manifestTmp, manifestPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException error) {
                throw new IOException("atomic checkpoint replacement is required but unsupported", error);
            }
            Format.forceDirectory(directory);
            if (previousLog != null) previousLog.close();
            Files.deleteIfExists(logPath);
            log = FileChannel.open(logPath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
            Format.writeFully(log, ByteBuffer.wrap(Format.WAL_MAGIC));
            log.force(true);
            Format.forceDirectory(directory);
            generation = nextGeneration;
        } catch (IOException error) {
            if (previousLog != null) {
                try { previousLog.close(); } catch (IOException ignored) { }
            }
            fail(error);
            throw error;
        }
    }

    @Override public void close() throws IOException {
        writer.lock();
        try {
            synchronized (lifecycle) {
                if (closed) return;
                closed = true;
            }
            if (log != null) { log.force(true); log.close(); }
            if (leaseLock != null) leaseLock.release();
            if (lease != null) lease.close();
        } finally { writer.unlock(); }
    }

    private void checkReadable() throws IOException { if (closed) throw new IOException("database is closed"); if (failure != null) throw failure; }
    private void checkWritable() throws IOException { checkReadable(); }
    private void checkReadableUnchecked() { if (closed || failure != null) throw new IllegalStateException("database is closed or failed", failure); }
    private void fail(IOException error) { if (failure == null) failure = new IOException("database entered failed state; reopen to recover", error); }
}
