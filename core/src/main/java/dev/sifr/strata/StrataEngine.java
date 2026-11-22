package dev.sifr.strata;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Single process, serialized writer.
 *
 * <p>Concurrency model: the live sorted map is guarded by the writer lock.
 * Point lookups take that lock. Snapshots and scans receive an immutable view
 * of the current map and mark it shared; the next mutation then copies once,
 * and subsequent mutations proceed in place until the next view is published.
 * Repeated scans with no intervening write therefore cost O(1) to open, while
 * write-heavy phases pay no copy. The original design copied the map on every
 * mutation, making per-write cost proportional to dataset size; see
 * docs/ENGINEERING.md.
 */
final class StrataEngine implements Strata {
    private final StrataOptions options;
    private final Path directory;
    private Path logPath;
    private Path segmentPath;
    private final Path manifestPath;
    private final ReentrantLock writer = new ReentrantLock();
    private FileChannel log;
    private FileChannel lease;
    private java.nio.channels.FileLock leaseLock;
    private NavigableMap<ByteKey, byte[]> values;
    /** True when {@link #values} has been handed to a reader view and must not be mutated. */
    private boolean shared;
    private long sequence;
    private long generation;
    private long liveBytes;
    /** Logical append offset inside the preallocated WAL. */
    private long walEnd;
    /** Bytes currently reserved for the WAL; data beyond {@link #walEnd} is zero filler. */
    private long walCapacity;
    private long segmentChecksum;
    private boolean closed;
    private IOException failure;

    /** Preallocation window: amortizes XFS delayed-allocation work across appends. */
    private static final long WAL_PREALLOCATE_BYTES = 1024 * 1024;

    private StrataEngine(StrataOptions options) {
        this.options = options;
        directory = options.directory().toAbsolutePath().normalize();

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
        Format.Checkpoint manifest = Format.readManifest(manifestPath);
        if (manifest != null) {
            generation = manifest.generation();
            segmentChecksum = manifest.segmentChecksum();
            segmentPath = segmentPath(generation);
            logPath = walPath(generation);
        }
        TreeMap<ByteKey, byte[]> recovered =
                segmentPath == null ? new TreeMap<>() : new TreeMap<>(Format.readSegment(segmentPath, options));
        if (manifest != null) {
            long actualChecksum = Format.segmentChecksum(segmentPath);
            if (actualChecksum != segmentChecksum) throw new IOException("segment identity does not match committed manifest");
        }
        sequence = manifest == null ? 0 : manifest.sequence();
        for (Mutation mutation : logPath == null ? List.<Mutation>of() : Format.readLog(logPath)) {
            Format.validate(mutation.key(), mutation.value(), options);
            applyTo(recovered, mutation);
            sequence++;
        }
        values = recovered;
        shared = false;
        liveBytes = liveBytes(values);
        // Any replay (including a truncated final frame) is checkpointed into a fresh WAL.
        // Never append after an incomplete record, and do not reapply an already checkpointed log.
        checkpointLocked(new TreeMap<>(values), sequence);
        // Explicit close owns resource lifetime; no per-open JVM shutdown hooks are accumulated.
    }

    private static void applyTo(NavigableMap<ByteKey, byte[]> target, Mutation mutation) {
        ByteKey key = new ByteKey(mutation.key());
        if (mutation.type() == Mutation.Type.PUT) target.put(key, mutation.value());
        else target.remove(key);
    }

    private static long liveBytes(NavigableMap<ByteKey, byte[]> map) {
        long total = 0;
        for (var entry : map.entrySet()) {
            total = Math.addExact(total, Math.addExact(entry.getKey().bytes.length, entry.getValue().length));
        }
        return total;
    }

    @Override public Optional<byte[]> get(byte[] key) throws IOException {
        Objects.requireNonNull(key, "key");
        writer.lock();
        try {
            checkReadable();
            byte[] value = values.get(new ByteKey(key));
            return Optional.ofNullable(value == null ? null : value.clone());
        } finally { writer.unlock(); }
    }

    @Override public void put(byte[] key, byte[] value) throws IOException { write(List.of(Mutation.put(key, value))); }
    @Override public void delete(byte[] key) throws IOException { write(List.of(Mutation.delete(key))); }

    @Override public void write(Iterable<Mutation> mutations) throws IOException {
        Objects.requireNonNull(mutations, "mutations");
        writer.lock();
        try {
            checkWritable();
            List<Mutation> batch = new ArrayList<>();
            long nextBytes = liveBytes;
            for (Mutation input : mutations) {
                Objects.requireNonNull(input, "mutation");
                Mutation mutation = new Mutation(input.type(), input.key(), input.value());
                Format.validate(mutation.key(), mutation.value(), options);
                ByteKey key = new ByteKey(mutation.key());
                byte[] old = values.get(key);
                if (old != null) nextBytes -= key.bytes.length + old.length;
                if (mutation.type() == Mutation.Type.PUT) nextBytes += mutation.key().length + mutation.value().length;
                if (shared) {
                    // A reader holds the published map. Copy exactly once, on the
                    // first mutation of this write, then continue in place. Doing
                    // this inside the loop keeps single-pass Iterables working.
                    values = new TreeMap<>(values);
                    shared = false;
                }
                applyTo(values, mutation);
                batch.add(mutation);
                sequence++;
            }
            if (batch.isEmpty()) return;
            if (nextBytes > options.maxLiveBytes()) throw new IOException("live-byte limit reached; compacting cannot reclaim live values");
            try {
                byte[] frame = Format.frame(batch);
                appendLocked(frame);
                if (options.durability() == StrataOptions.Durability.SYNC) log.force(false);
            } catch (IOException error) {
                // The map already advanced; mark the failed state so no reader
                // can silently observe memory that is not durable.
                fail(error);
                throw error;
            }
            liveBytes = nextBytes;
            if (walEnd >= options.maxSegmentBytes()) compactLocked();
        } finally { writer.unlock(); }
    }

    @Override public Snapshot snapshot() {
        return new StateSnapshot(view());
    }

    @Override public Scan scan(byte[] from, byte[] to, long limit) {
        return new StateScan(view(), from, to, limit, this::checkReadableUnchecked);
    }

    /** A point-in-time immutable view; subsequent mutations will copy on write. */
    private StorageState view() {
        writer.lock();
        try {
            checkReadableUnchecked();
            shared = true;
            return new StorageState(values, sequence);
        } finally { writer.unlock(); }
    }

    @Override public void compact() throws IOException {
        writer.lock();
        try { checkWritable(); compactLocked(); }
        finally { writer.unlock(); }
    }

    /**
     * Append at the logical end. The WAL is preallocated in fixed windows so
     * that a durable write does not force the filesystem to allocate on the
     * fsync path; unwritten capacity is zero-filled and terminates recovery.
     */
    private void appendLocked(byte[] frame) throws IOException {
        if (walEnd + frame.length > walCapacity) {
            ensureCapacityLocked(walEnd + frame.length);
        }
        ByteBuffer buffer = ByteBuffer.wrap(frame);
        long position = walEnd;
        while (buffer.hasRemaining()) {
            position += log.write(buffer, position);
        }
        walEnd = position;
    }

    private void extendFileLocked(long target) throws IOException {
        extendFile(log, target);
    }

    /** Extend with zeros. FileChannel.truncate does not grow a file. */
    private static void extendFile(FileChannel channel, long target) throws IOException {
        if (channel.size() >= target) return;
        ByteBuffer zero = ByteBuffer.allocate(1);
        while (zero.hasRemaining()) channel.write(zero, target - 1);
    }

    private void ensureCapacityLocked(long required) throws IOException {
        long target = Math.max(required, walCapacity + WAL_PREALLOCATE_BYTES);
        extendFileLocked(target);
        walCapacity = target;
    }

    private void compactLocked() throws IOException {
        checkpointLocked(new TreeMap<>(values), sequence);
    }

    private void checkpoint(NavigableMap<ByteKey, byte[]> checkpoint, long checkpointSequence) throws IOException {
        writer.lock();
        try { checkpointLocked(checkpoint, checkpointSequence); }
        finally { writer.unlock(); }
    }

    private void checkpointLocked(NavigableMap<ByteKey, byte[]> checkpoint, long checkpointSequence) throws IOException {
        FileChannel previousLog = log;
        try {
            long nextGeneration = generation + 1;
            Path nextSegment = segmentPath(nextGeneration);
            Path nextWal = walPath(nextGeneration);
            Path segmentTmp = directory.resolve("segment.tmp");
            Path manifestTmp = directory.resolve("MANIFEST.tmp");
            Path walTmp = directory.resolve("wal.tmp");
            Files.deleteIfExists(segmentTmp);
            Files.deleteIfExists(manifestTmp);
            Files.deleteIfExists(walTmp);
            long checksum = Format.writeSegment(segmentTmp, checkpoint);
            Format.writeManifest(manifestTmp, checkpointSequence, nextGeneration, checksum);
            try (FileChannel wal = FileChannel.open(walTmp, StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                Format.writeFully(wal, ByteBuffer.wrap(Format.WAL_MAGIC));
                // Reserve the first window up front; the zero tail is a valid
                // end-of-log marker for recovery.
                extendFile(wal, WAL_PREALLOCATE_BYTES + Format.WAL_MAGIC.length);
                wal.force(true);
            }
            try {
                // Generation-scoped names make an uncommitted segment/WAL harmless after a crash.
                Files.move(segmentTmp, nextSegment, StandardCopyOption.ATOMIC_MOVE);
                Files.move(walTmp, nextWal, StandardCopyOption.ATOMIC_MOVE);
                // This single manifest replacement selects one complete generation atomically.
                Files.move(manifestTmp, manifestPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException error) {
                throw new IOException("atomic checkpoint replacement is required but unsupported", error);
            }
            Format.forceDirectory(directory);
            log = FileChannel.open(nextWal, StandardOpenOption.READ, StandardOpenOption.WRITE);
            walEnd = Format.WAL_MAGIC.length;
            walCapacity = WAL_PREALLOCATE_BYTES + Format.WAL_MAGIC.length;
            if (previousLog != null) previousLog.close();
            segmentPath = nextSegment;
            logPath = nextWal;
            generation = nextGeneration;
            segmentChecksum = checksum;
            // Retain obsolete generation files for diagnostic recovery in this 0.x milestone.
            // Documented disk-growth policy is required before automatic reclamation.
        } catch (IOException error) {
            if (previousLog != null) {
                try { previousLog.close(); } catch (IOException ignored) { }
            }
            fail(error);
            throw error;
        }
    }

    private Path segmentPath(long value) { return directory.resolve("segment-" + value + ".sst"); }
    private Path walPath(long value) { return directory.resolve("wal-" + value + ".log"); }

    @Override public void close() throws IOException {
        writer.lock();
        try {
            if (closed) return;
            closed = true;
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
