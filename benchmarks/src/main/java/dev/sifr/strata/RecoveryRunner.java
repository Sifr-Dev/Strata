package dev.sifr.strata;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

/**
 * Recovery and crash measurement.
 *
 * <ul>
 *   <li>{@code RecoveryRunner reopen <dir> <entries>}: measures the time from a
 *       cold {@code Strata.open} to the first successful read of every key, and
 *       verifies the full contents.</li>
 *   <li>{@code RecoveryRunner crash <dir> <acked>}: starts a child JVM that
 *       appends {@code acked} SYNC writes and reports each one, SIGKILLs it, then
 *       reopens and verifies that pre-existing data and every acknowledged write
 *       survived. Lost acknowledged writes are reported as an error.</li>
 * </ul>
 */
public final class RecoveryRunner {

    static byte[] key(int index) {
        return ByteBuffer.allocate(8).putLong(index).array();
    }

    public static void main(String[] argv) throws Exception {
        switch (argv[0]) {
            case "child" -> child(Path.of(argv[1]), Integer.parseInt(argv[2]), Integer.parseInt(argv[3]));
            case "reopen" -> reopen(Path.of(argv[1]), Integer.parseInt(argv[2]));
            case "crash" -> crash(Path.of(argv[1]), Integer.parseInt(argv[2]), Integer.parseInt(argv[3]));
            default -> {
                System.err.println("usage: reopen|crash <dir> <entries> [acked]");
                System.exit(2);
            }
        }
    }

    /** Child: append acknowledged SYNC writes starting at {@code start} for release-mode data. */
    static void child(Path dir, int start, int count) throws Exception {
        try (Strata db = Strata.open(StrataOptions.builder(dir)
                .durability(StrataOptions.Durability.SYNC).build())) {
            for (int i = 0; i < count; i++) {
                db.put(key(start + i), ("crash-" + (start + i)).getBytes(StandardCharsets.US_ASCII));
                System.out.println("ACK " + (start + i));
                System.out.flush();
            }
        }
    }

    static void reopen(Path dir, int entries) throws Exception {
        long start = System.nanoTime();
        try (Strata db = Strata.open(StrataOptions.builder(dir)
                .durability(StrataOptions.Durability.SYNC).build())) {
            for (int i = 0; i < entries; i++) {
                if (db.get(key(i)).isEmpty()) throw new AssertionError("missing key " + i);
            }
            long elapsed = System.nanoTime() - start;
            System.out.printf(Locale.ROOT,
                    "{\"workload\":\"reopen_verify\",\"entries\":%d,\"elapsed_ms\":%.3f,\"per_entry_us\":%.3f}%n",
                    entries, elapsed / 1e6, elapsed / 1000.0 / entries);
        }
    }

    static void crash(Path dir, int baseEntries, int acked) throws Exception {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        Process child = new ProcessBuilder(java, "-cp", System.getProperty("java.class.path"),
                RecoveryRunner.class.getName(), "child", dir.toString(),
                Integer.toString(baseEntries), Integer.toString(acked))
                .redirectErrorStream(true).start();
        int received = 0;
        try (BufferedReader lines = new BufferedReader(new InputStreamReader(child.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = lines.readLine()) != null) {
                if (!line.startsWith("ACK ")) {
                    System.out.println("child: " + line);
                    continue;
                }
                received++;
                if (received >= acked) break;
            }
        }
        child.destroyForcibly();  // SIGKILL on Linux
        child.waitFor();
        if (received != acked) throw new AssertionError("child acknowledged " + received + "/" + acked);

        long start = System.nanoTime();
        try (Strata db = Strata.open(StrataOptions.builder(dir)
                .durability(StrataOptions.Durability.SYNC).build())) {
            for (int i = 0; i < baseEntries; i++) {
                if (db.get(key(i)).isEmpty()) throw new AssertionError("lost pre-existing key " + i);
            }
            for (int i = 0; i < acked; i++) {
                if (db.get(key(baseEntries + i)).isEmpty()) throw new AssertionError("lost acknowledged key " + i);
            }
            long elapsed = System.nanoTime() - start;
            System.out.printf(Locale.ROOT,
                    "{\"workload\":\"crash_recovery\",\"pre_existing\":%d,\"acked\":%d,"
                            + "\"lost_acknowledged\":0,\"recovery_ms\":%.3f}%n",
                    baseEntries, acked, elapsed / 1e6);
        }
        long size;
        try (var walk = Files.walk(dir)) {
            size = walk.filter(Files::isRegularFile).mapToLong(p -> {
                try { return Files.size(p); } catch (Exception e) { return 0; }
            }).sum();
        }
        System.out.printf(Locale.ROOT, "{\"bytes_on_disk\":%d}%n", size);
    }

    private RecoveryRunner() { }
}
