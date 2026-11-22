package dev.sifr.strata;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Random;

/**
 * End-to-end workload harness with explicit durability, percentile and
 * recovery measurement. Intentionally not JMH: these workloads include real
 * file I/O and must report tail latency over every sample.
 *
 * <p>Both engines are driven through an adapter with identical call sequences:
 * one key-value mutation per operation, the same key/value sizes, the same
 * pseudo-random order and the same durability class. SQLite runs in WAL mode
 * with {@code synchronous=NORMAL} for the FLUSH_ONLY class and
 * {@code synchronous=FULL} for SYNC.
 *
 * <p>Usage: {@code Runner <engine> <workload> <dir> <entries> <durability> [samples]}
 * where engine is {@code strata} or {@code sqlite}, durability is
 * {@code FLUSH_ONLY} or {@code SYNC}.
 */
public final class WorkloadRunner {

    interface Engine extends AutoCloseable {
        void put(byte[] key, byte[] value) throws Exception;

        void delete(byte[] key) throws Exception;

        byte[] get(byte[] key) throws Exception;

        int scan(byte[] from, byte[] to, int limit) throws Exception;

        void batch(List<byte[]> keys, byte[][] values) throws Exception;

        long compact() throws Exception;

        void close() throws Exception;
    }

    static final class StrataEngineAdapter implements Engine {
        private final Strata db;

        StrataEngineAdapter(Path dir, StrataOptions.Durability durability) throws IOException {
            db = Strata.open(StrataOptions.builder(dir).durability(durability).build());
        }

        public void put(byte[] key, byte[] value) throws IOException { db.put(key, value); }

        public void delete(byte[] key) throws IOException { db.delete(key); }

        public byte[] get(byte[] key) throws IOException { return db.get(key).orElse(null); }

        public int scan(byte[] from, byte[] to, int limit) throws IOException {
            int count = 0;
            try (Scan scan = db.scan(from, to, limit)) {
                while (scan.next()) count++;
            }
            return count;
        }

        public void batch(List<byte[]> keys, byte[][] values) throws IOException {
            List<Mutation> mutations = new ArrayList<>(keys.size());
            for (int i = 0; i < keys.size(); i++) mutations.add(Mutation.put(keys.get(i), values[i]));
            db.write(mutations);
        }

        public long compact() throws IOException {
            long start = System.nanoTime();
            db.compact();
            return System.nanoTime() - start;
        }

        public void close() throws IOException { db.close(); }
    }

    static final class SqliteEngineAdapter implements Engine {
        private final Connection connection;
        private PreparedStatement put;
        private PreparedStatement get;
        private PreparedStatement delete;
        private PreparedStatement scan;

        SqliteEngineAdapter(Path dir, boolean sync) throws Exception {
            connection = DriverManager.getConnection("jdbc:sqlite:" + dir.resolve("sqlite.db"));
            try (var statement = connection.createStatement()) {
                statement.execute("PRAGMA journal_mode=WAL");
                statement.execute("PRAGMA page_size=4096");
                statement.execute("PRAGMA synchronous=" + (sync ? "FULL" : "NORMAL"));
                statement.execute("PRAGMA wal_autocheckpoint=0");
                statement.execute("CREATE TABLE IF NOT EXISTS kv (k BLOB PRIMARY KEY NOT NULL, v BLOB NOT NULL) WITHOUT ROWID");
            }
            prepareStatements();
        }

        private void prepareStatements() throws Exception {
            put = connection.prepareStatement("INSERT OR REPLACE INTO kv(k, v) VALUES (?, ?)");
            get = connection.prepareStatement("SELECT v FROM kv WHERE k = ?");
            delete = connection.prepareStatement("DELETE FROM kv WHERE k = ?");
            scan = connection.prepareStatement("SELECT k, v FROM kv WHERE k >= ? AND k < ? ORDER BY k LIMIT ?");
        }

        public void put(byte[] key, byte[] value) throws Exception {
            if ("1".equals(System.getenv("SQDEBUG")) && System.nanoTime() % 5000 < 100000) System.err.println("autocommit=" + connection.getAutoCommit() + " tx=" + connection.getTransactionIsolation());
            put.setBytes(1, key);
            put.setBytes(2, value);
            put.executeUpdate();
        }

        public void delete(byte[] key) throws Exception {
            delete.setBytes(1, key);
            delete.executeUpdate();
        }

        public byte[] get(byte[] key) throws Exception {
            get.setBytes(1, key);
            try (ResultSet rows = get.executeQuery()) {
                return rows.next() ? rows.getBytes(1) : null;
            }
        }

        public int scan(byte[] from, byte[] to, int limit) throws Exception {
            scan.setBytes(1, from);
            scan.setBytes(2, to);
            scan.setInt(3, limit);
            int count = 0;
            try (ResultSet rows = scan.executeQuery()) {
                while (rows.next()) count++;
            }
            return count;
        }

        public void batch(List<byte[]> keys, byte[][] values) throws Exception {
            connection.setAutoCommit(false);
            try {
                for (int i = 0; i < keys.size(); i++) {
                    put.setBytes(1, keys.get(i));
                    put.setBytes(2, values[i]);
                    put.addBatch();
                }
                put.executeBatch();
                connection.commit();
            } finally {
                connection.setAutoCommit(true);
            }
        }

        /** SQLite has no explicit full rewrite; VACUUM is the equivalent operation. */
        public long compact() throws Exception {
            // VACUUM refuses to run while other statements are active on this
            // connection, so the prepared handles are dropped and re-created.
            put.close();
            get.close();
            delete.close();
            scan.close();
            long start = System.nanoTime();
            try (var statement = connection.createStatement()) {
                statement.execute("VACUUM");
            }
            prepareStatements();
            return System.nanoTime() - start;
        }

        public void close() throws Exception {
            connection.close();
        }
    }

    static byte[] key(int index) {
        return ByteBuffer.allocate(8).putLong(index).array();
    }

    /**
     * Deterministic counter-based value bytes. Rewrites must differ from the
     * existing value: SQLite skips the write (and its fsync) when an
     * INSERT OR REPLACE stores a byte-identical row, which otherwise makes a
     * "rewrite" workload look ~130x cheaper than a real overwrite.
     */
    static byte[] value(long seed, int size) {
        byte[] out = new byte[size];
        long state = seed * 0x9E3779B97F4A7C15L + 0x2545F4914F6CDD1DL;
        for (int i = 0; i < size; i++) {
            state ^= state << 13;
            state ^= state >>> 7;
            state ^= state << 17;
            out[i] = (byte) state;
        }
        return out;
    }

    static final class Percentiles {
        final long[] sorted;
        Percentiles(long[] samples) {
            sorted = samples.clone();
            Arrays.sort(sorted);
        }
        long at(double q) {
            if (sorted.length == 0) return 0;
            int index = (int) Math.min(sorted.length - 1L, Math.max(0L, Math.round(q * (sorted.length - 1))));
            return sorted[index];
        }
        double mean() {
            if (sorted.length == 0) return 0;
            double sum = 0;
            for (long value : sorted) sum += value;
            return sum / sorted.length;
        }
    }

    static void emit(PrintStream out, String engine, String workload, String durability, int entries,
                     int valueSize, int samples, long elapsedNanos, long[] operations) {
        Percentiles p = new Percentiles(operations);
        double throughput = elapsedNanos == 0 ? 0 : operations.length / (elapsedNanos / 1e9);
        out.printf(Locale.ROOT,
                "{\"engine\":\"%s\",\"workload\":\"%s\",\"durability\":\"%s\",\"entries\":%d,"
                        + "\"value_size\":%d,\"samples\":%d,\"ops\":%d,\"elapsed_ms\":%.3f,"
                        + "\"throughput_ops_s\":%.1f,\"mean_us\":%.3f,\"p50_us\":%.3f,\"p95_us\":%.3f,"
                        + "\"p99_us\":%.3f,\"p999_us\":%.3f,\"max_us\":%.3f}%n",
                engine, workload, durability, entries, valueSize, samples, operations.length,
                elapsedNanos / 1e6, throughput, p.mean() / 1000.0,
                p.at(0.50) / 1000.0, p.at(0.95) / 1000.0, p.at(0.99) / 1000.0,
                p.at(0.999) / 1000.0, p.at(1.0) / 1000.0);
        out.flush();
    }

    public static void main(String[] argv) throws Exception {
        if (argv.length < 5) {
            System.err.println("usage: Runner <strata|sqlite> <workload> <dir> <entries> <FLUSH_ONLY|SYNC> [samples]");
            System.exit(2);
        }
        String engineName = argv[0];
        String workload = argv[1];
        Path dir = Path.of(argv[2]);
        int entries = Integer.parseInt(argv[3]);
        String durability = argv[4].toUpperCase(Locale.ROOT);
        int samples = argv.length > 5 ? Integer.parseInt(argv[5]) : Math.max(entries, 1);
        int valueSize = 128;
        boolean sync = durability.equals("SYNC");
        // Base values for fill; writes derive new unique values per operation.
        byte[][] values = new byte[Math.max(entries, 64)][];
        for (int i = 0; i < values.length; i++) values[i] = value(i, valueSize);

        Files.createDirectories(dir);
        PrintStream out = System.out;
        long[] operations = new long[Math.max(samples, 1)];
        try (Engine engine = engineName.equals("sqlite")
                ? new SqliteEngineAdapter(dir, sync)
                : new StrataEngineAdapter(dir, sync ? StrataOptions.Durability.SYNC : StrataOptions.Durability.FLUSH_ONLY)) {

            // Populate once so read workloads have data; fill itself is measured
            // separately so its cost never contaminates another workload.
            long fillStart = System.nanoTime();
            for (int i = 0; i < entries; i++) engine.put(key(i), values[i % values.length]);
            long writeEpoch = entries;
            long fillElapsed = System.nanoTime() - fillStart;
            if (workload.equals("fill")) {
                long[] fillOps = new long[entries];
                Arrays.fill(fillOps, Math.max(1, fillElapsed / Math.max(entries, 1)));
                emit(out, engineName, "fill", durability, entries, valueSize, entries, fillElapsed, fillOps);
                return;
            }

            Random random = new Random(42);
            int count = Math.min(samples, workload.equals("randread") || workload.equals("randwrite")
                    ? entries : Math.max(samples, 1));

            if (workload.equals("randread")) {
                long[] latencies = new long[count];
                long start = System.nanoTime();
                for (int i = 0; i < count; i++) {
                    long opStart = System.nanoTime();
                    engine.get(key(random.nextInt(entries)));
                    latencies[i] = System.nanoTime() - opStart;
                }
                emit(out, engineName, workload, durability, entries, valueSize, count, System.nanoTime() - start, latencies);
            } else if (workload.equals("seqread")) {
                long[] latencies = new long[count];
                long start = System.nanoTime();
                for (int i = 0; i < count; i++) {
                    long opStart = System.nanoTime();
                    engine.get(key(i % entries));
                    latencies[i] = System.nanoTime() - opStart;
                }
                emit(out, engineName, workload, durability, entries, valueSize, count, System.nanoTime() - start, latencies);
            } else if (workload.equals("seqwrite")) {
                long start = System.nanoTime();
                for (int i = 0; i < count; i++) {
                    long opStart = System.nanoTime();
                    engine.put(key(i % entries), value(writeEpoch + i, valueSize));
                    operations[i] = System.nanoTime() - opStart;
                }
                emit(out, engineName, workload, durability, entries, valueSize, count, System.nanoTime() - start, operations);
            } else if (workload.equals("randwrite")) {
                long start = System.nanoTime();
                for (int i = 0; i < count; i++) {
                    long opStart = System.nanoTime();
                    engine.put(key(random.nextInt(entries)), value(writeEpoch + i, valueSize));
                    operations[i] = System.nanoTime() - opStart;
                }
                emit(out, engineName, workload, durability, entries, valueSize, count, System.nanoTime() - start, operations);
            } else if (workload.equals("batchwrite")) {
                int batchSize = 100;
                int batches = Math.max(1, Math.min(samples, entries / batchSize));
                long start = System.nanoTime();
                for (int b = 0; b < batches; b++) {
                    List<byte[]> keys = new ArrayList<>(batchSize);
                    byte[][] batchValues = new byte[batchSize][];
                    for (int j = 0; j < batchSize; j++) {
                        keys.add(key((b * batchSize + j) % entries));
                        batchValues[j] = value(writeEpoch + b, valueSize);
                    }
                    long opStart = System.nanoTime();
                    engine.batch(keys, batchValues);
                    operations[b] = System.nanoTime() - opStart;
                }
                long[] sampled = Arrays.copyOf(operations, batches);
                emit(out, engineName, workload, durability, entries, valueSize, batches, System.nanoTime() - start, sampled);
            } else if (workload.equals("scan100") || workload.equals("scan1000")) {
                int limit = workload.equals("scan100") ? 100 : 1000;
                long start = System.nanoTime();
                for (int i = 0; i < count; i++) {
                    long opStart = System.nanoTime();
                    int from = random.nextInt(Math.max(1, entries - limit));
                    engine.scan(key(from), key(from + limit), limit);
                    operations[i] = System.nanoTime() - opStart;
                }
                emit(out, engineName, workload, durability, entries, valueSize, count, System.nanoTime() - start, operations);
            } else if (workload.equals("mixed")) {
                long start = System.nanoTime();
                for (int i = 0; i < count; i++) {
                    long opStart = System.nanoTime();
                    if ((i & 1) == 0) engine.put(key(random.nextInt(entries)), value(writeEpoch + i, valueSize));
                    else engine.get(key(random.nextInt(entries)));
                    operations[i] = System.nanoTime() - opStart;
                }
                emit(out, engineName, workload, durability, entries, valueSize, count, System.nanoTime() - start, operations);
            } else if (workload.equals("compact")) {
                long elapsed = engine.compact();
                long[] one = { elapsed };
                emit(out, engineName, workload, durability, entries, valueSize, 1, elapsed, one);
            } else {
                System.err.println("unknown workload: " + workload);
                System.exit(2);
            }
        }
    }
}
