package dev.sifr.strata;

import java.util.Arrays;
import java.util.Objects;

/** An immutable put or delete; arrays are copied at the public boundary. */
public record Mutation(Type type, byte[] key, byte[] value) {
    public enum Type { PUT, DELETE }
    public Mutation {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(key, "key");
        key = key.clone();
        if (type == Type.PUT) value = Objects.requireNonNull(value, "value").clone();
        else if (value != null) throw new IllegalArgumentException("delete mutations cannot contain a value");
    }
    public static Mutation put(byte[] key, byte[] value) { return new Mutation(Type.PUT, key, value); }
    public static Mutation delete(byte[] key) { return new Mutation(Type.DELETE, key, null); }
    @Override public byte[] key() { return key.clone(); }
    @Override public byte[] value() { return value == null ? null : value.clone(); }
    @Override public boolean equals(Object other) {
        return other instanceof Mutation m && type == m.type && Arrays.equals(key, m.key) && Arrays.equals(value, m.value);
    }
    @Override public int hashCode() { return 31 * (31 * type.hashCode() + Arrays.hashCode(key)) + Arrays.hashCode(value); }
}
