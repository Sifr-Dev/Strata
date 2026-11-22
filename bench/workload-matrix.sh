#!/usr/bin/env bash
# Strata end-to-end workload matrix, including the SQLite comparator.
#
# Usage: bench/workload-matrix.sh <raw-output-dir> [entries] [read-samples] [write-samples]
#
# Engines: strata, sqlite (both driven by WorkloadRunner with identical call
# sequences, key/value sizes, random order and durability class).
# Serial execution by design (16 GiB host).
set -euo pipefail
cd "$(dirname "$0")/.."   # strata repo root

ENGINE_JAR="$PWD/benchmarks/target/strata-benchmarks-0.1.0-SNAPSHOT.jar"
OUT="${1:?raw output directory required}"
ENTRIES="${2:-20000}"
READ_SAMPLES="${3:-20000}"
WRITE_SAMPLES="${4:-4000}"
RUN_ID="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
mkdir -p "$OUT"
BASE="$(mktemp -d "$OUT/data-XXXXXX")"

{
  echo "# strata workload matrix, run $RUN_ID"
  echo "# host: $(uname -sr),$(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2)"
  echo "# jdk: $(java -version 2>&1 | head -1)"
  echo "# fs: $(findmnt -no FSTYPE -T "$OUT")"
  echo "# entries: $ENTRIES, value: 128 B, read samples: $READ_SAMPLES, write samples: $WRITE_SAMPLES"
  echo "# durability classes: FLUSH_ONLY -> SQLite WAL+synchronous=NORMAL, SYNC -> WAL+synchronous=FULL"
  echo "# engine jar sha256: $(sha256sum "$ENGINE_JAR" | cut -d' ' -f1)"
} > "$OUT/run-info.txt"

WORKLOADS_READ="seqread randread scan100 scan1000"
WORKLOADS_WRITE="seqwrite randwrite mixed batchwrite"

for durability in FLUSH_ONLY SYNC; do
  for engine in strata sqlite; do
    echo "== $engine $durability" | tee -a "$OUT/run-info.txt"
    DIR="$BASE/$engine-$durability"
    mkdir -p "$DIR"
    # fill first (measured separately, never inside another workload's timings)
    java --enable-native-access=ALL-UNNAMED -cp "$ENGINE_JAR" dev.sifr.strata.WorkloadRunner \
      "$engine" fill "$DIR" "$ENTRIES" "$durability" | tee -a "$OUT/results.jsonl"
    for w in $WORKLOADS_READ; do
      java --enable-native-access=ALL-UNNAMED -cp "$ENGINE_JAR" dev.sifr.strata.WorkloadRunner \
        "$engine" "$w" "$DIR" "$ENTRIES" "$durability" "$READ_SAMPLES" | tee -a "$OUT/results.jsonl"
    done
    for w in $WORKLOADS_WRITE; do
      java --enable-native-access=ALL-UNNAMED -cp "$ENGINE_JAR" dev.sifr.strata.WorkloadRunner \
        "$engine" "$w" "$DIR" "$ENTRIES" "$durability" "$WRITE_SAMPLES" | tee -a "$OUT/results.jsonl"
    done
    java --enable-native-access=ALL-UNNAMED -cp "$ENGINE_JAR" dev.sifr.strata.WorkloadRunner \
      "$engine" compact "$DIR" "$ENTRIES" "$durability" | tee -a "$OUT/results.jsonl"
    rm -rf "$DIR"
  done
done

echo "== recovery" | tee -a "$OUT/run-info.txt"
REC="$BASE/recovery"
mkdir -p "$REC"
java --enable-native-access=ALL-UNNAMED -cp "$ENGINE_JAR" dev.sifr.strata.WorkloadRunner \
  strata fill "$REC" "$ENTRIES" SYNC | tee -a "$OUT/results.jsonl"
java --enable-native-access=ALL-UNNAMED -cp "$ENGINE_JAR" dev.sifr.strata.RecoveryRunner \
  reopen "$REC" "$ENTRIES" | tee -a "$OUT/results.jsonl"
mkdir -p "$REC-crash"
java --enable-native-access=ALL-UNNAMED -cp "$ENGINE_JAR" dev.sifr.strata.WorkloadRunner \
  strata fill "$REC-crash" 5000 SYNC > /dev/null
java --enable-native-access=ALL-UNNAMED -cp "$ENGINE_JAR" dev.sifr.strata.RecoveryRunner \
  crash "$REC-crash" 5000 200 | tee -a "$OUT/results.jsonl"
rm -rf "$REC" "$REC-crash" "$BASE"
echo "matrix complete"
