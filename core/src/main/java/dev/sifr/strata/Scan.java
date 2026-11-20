package dev.sifr.strata;

/** Iteration stops cleanly at the requested bound; close releases segment readers. */
public interface Scan extends AutoCloseable, Iterable<Entry> {
    boolean next() throws java.io.IOException;
    Entry entry();
    @Override default java.util.Iterator<Entry> iterator() {
        return new java.util.Iterator<>() {
            private Entry next;
            @Override public boolean hasNext() {
                if (next != null) return true;
                try { if (!Scan.this.next()) return false; next = Scan.this.entry(); return true; }
                catch (java.io.IOException e) { throw new java.io.UncheckedIOException(e); }
            }
            @Override public Entry next() {
                if (!hasNext()) throw new java.util.NoSuchElementException();
                Entry result = next; next = null; return result;
            }
        };
    }
    @Override void close();
}
