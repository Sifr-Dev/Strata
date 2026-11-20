package dev.sifr.strata.cli;

import dev.sifr.strata.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HexFormat;

/** Small administration client; arguments are never concatenated into a shell. */
public final class Main {
    private Main() {}
    public static void main(String[] args) {
        try { run(args); }
        catch (Exception error) { System.err.println("strata: " + error.getMessage()); System.exit(1); }
    }
    static void run(String[] args) throws Exception {
        if (args.length < 2) { usage(); return; }
        Path path = Path.of(args[0]);
        switch (args[1]) {
            case "put" -> { require(args, 4, "put KEY VALUE"); try (Strata db = Strata.open(path)) { db.put(bytes(args[2]), bytes(args[3])); } }
            case "get" -> { require(args, 3, "get KEY"); try (Strata db = Strata.open(path)) { db.get(bytes(args[2])).ifPresentOrElse(value -> System.out.println(new String(value, StandardCharsets.UTF_8)), () -> System.exit(3)); } }
            case "delete" -> { require(args, 3, "delete KEY"); try (Strata db = Strata.open(path)) { db.delete(bytes(args[2])); } }
            case "scan" -> { require(args, 4, "scan FROM_EXCLUSIVE_KEY TO_EXCLUSIVE_KEY"); try (Strata db = Strata.open(path); Scan scan = db.scan(bytes(args[2]), bytes(args[3]), 1000)) { while (scan.next()) { Entry e = scan.entry(); System.out.println(HexFormat.of().formatHex(e.key()) + " " + HexFormat.of().formatHex(e.value())); } } }
            case "compact" -> { try (Strata db = Strata.open(path)) { db.compact(); } }
            default -> usage();
        }
    }
    private static byte[] bytes(String text) { return text.getBytes(StandardCharsets.UTF_8); }
    private static void require(String[] args, int count, String usage) { if (args.length != count) throw new IllegalArgumentException("expected " + usage); }
    private static void usage() { System.out.println("usage: strata DIRECTORY put KEY VALUE | get KEY | delete KEY | scan FROM TO | compact"); }
}
