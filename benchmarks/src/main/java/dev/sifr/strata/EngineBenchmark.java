package dev.sifr.strata;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** Explicit in-memory and durable workloads; not a competitor comparison. */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(3)
@State(Scope.Benchmark)
public class EngineBenchmark {
    @Param({"10000"})
    int entries;
    @Param({"FLUSH_ONLY", "SYNC"})
    String durability;

    Strata db;
    Path directory;
    byte[][] keys;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        directory = Files.createTempDirectory("strata-jmh");
        db = Strata.open(StrataOptions.builder(directory)
                .durability(StrataOptions.Durability.valueOf(durability)).build());
        keys = new byte[entries][];
        for (int i = 0; i < entries; i++) {
            keys[i] = String.format(Locale.ROOT, "%08d", i).getBytes(java.nio.charset.StandardCharsets.US_ASCII);
            db.put(keys[i], new byte[128]);
        }
    }
    @TearDown(Level.Trial)
    public void teardown() throws IOException {
        db.close();
        try (var paths = Files.walk(directory)) {
            paths.sorted(Comparator.reverseOrder()).forEach(path -> { try { Files.delete(path); } catch (IOException e) { throw new RuntimeException(e); } });
        }
    }
    @Benchmark public void getExisting(Blackhole sink) throws IOException { sink.consume(db.get(keys[entries / 2])); }
    @Benchmark public void getMissing(Blackhole sink) throws IOException { sink.consume(db.get(new byte[]{127})); }
    @Benchmark public Strata putSmall() throws IOException { db.put(keys[0], new byte[128]); return db; }
    @Benchmark public Strata writeBatch() throws IOException {
        List<Mutation> batch = new ArrayList<>();
        for (int i = 0; i < 100; i++) batch.add(Mutation.put(keys[i], new byte[128]));
        db.write(batch); return db;
    }
    @Benchmark public int scanHundred(Blackhole sink) throws IOException {
        try (Scan scan = db.scan(new byte[]{0}, new byte[]{127}, 100)) {
            int count = 0; while (scan.next()) { sink.consume(scan.entry()); count++; } return count;
        }
    }
    @Benchmark public Strata compact() throws IOException { db.compact(); return db; }
}
