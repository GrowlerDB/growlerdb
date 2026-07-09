package io.growlerdb.connector;

import io.growlerdb.proto.v1.Coordinates;
import io.growlerdb.proto.v1.Field;
import io.growlerdb.proto.v1.Value;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * JVM port of {@code growlerdb_core::ShardRouter} (task-29): map a composite key
 * ({@link Coordinates}) to a shard ordinal in {@code [0, shards)}. The connector uses this
 * to <b>place writes</b> on the same shard the Gateway's reader routes a key lookup to — so
 * a document is written where it will later be searched.
 *
 * <p><b>This must stay byte-for-byte identical to the Rust router.</b> The key encoding
 * ({@link #encode}), the FNV-1a hash ({@link #fnv1a}), and the strategy below mirror
 * {@code crates/growlerdb-core/src/{doc,routing}.rs}. {@code ShardRouterParityTest} asserts
 * the same golden vectors the Rust side asserts, so any drift fails a test rather than
 * silently misplacing writes relative to reads.
 */
public final class ShardRouter implements java.io.Serializable {

  /** How a key maps to a shard — mirrors Rust {@code RoutingStrategy}. */
  public enum Strategy {
    /** Hash the full key — uniform spread (the default for an unpartitioned index). */
    HASH,
    /** Hash the key's partition fields, co-locating a partition; falls back to the full key. */
    PARTITION
  }

  /**
   * Number of <b>virtual buckets</b> (task-77) — mirrors Rust {@code routing::NUM_BUCKETS}. Keys
   * hash into {@code [0, NUM_BUCKETS)} and a {@code bucket→shard} map assigns owners, so resharding
   * moves whole buckets instead of re-routing every key. Must match the Rust constant exactly.
   */
  public static final int NUM_BUCKETS = 1024;

  private final int shards;
  private final Strategy strategy;
  /** Bucket→shard owners for a bucketed router (task-77); {@code null} ⇒ legacy {@code fnv % shards}. */
  private final int[] bucketOwners;

  public ShardRouter(int shards, Strategy strategy) {
    this.shards = Math.max(1, shards);
    this.strategy = strategy;
    this.bucketOwners = null;
  }

  private ShardRouter(Strategy strategy, int[] bucketOwners) {
    this.strategy = strategy;
    this.bucketOwners = bucketOwners;
    int max = 0;
    for (int o : bucketOwners) {
      max = Math.max(max, o);
    }
    this.shards = max + 1;
  }

  /** Legacy hash routing over {@code shards} shards. */
  public static ShardRouter hashed(int shards) {
    return new ShardRouter(shards, Strategy.HASH);
  }

  /** Legacy partition routing over {@code shards} shards. */
  public static ShardRouter partitioned(int shards) {
    return new ShardRouter(shards, Strategy.PARTITION);
  }

  /**
   * A <b>bucketed</b> router (task-77): keys hash to a bucket, {@code bucketOwners[bucket]} names
   * its shard. Mirrors Rust {@code ShardRouter::bucketed}; {@code bucketOwners} comes from the
   * registry's {@link io.growlerdb.proto.v1 bucket map} (length must be {@link #NUM_BUCKETS}).
   */
  public static ShardRouter bucketed(Strategy strategy, int[] bucketOwners) {
    if (bucketOwners.length != NUM_BUCKETS) {
      throw new IllegalArgumentException(
          "bucket map must have exactly " + NUM_BUCKETS + " entries, got " + bucketOwners.length);
    }
    return new ShardRouter(strategy, bucketOwners.clone());
  }

  /** The default <b>balanced</b> bucket map over {@code shards} shards: round-robin {@code b % shards}. */
  public static int[] balancedBucketMap(int shards) {
    int n = Math.max(1, shards);
    int[] owners = new int[NUM_BUCKETS];
    for (int b = 0; b < NUM_BUCKETS; b++) {
      owners[b] = b % n;
    }
    return owners;
  }

  public int shards() {
    return shards;
  }

  /** The bytes hashed for routing under this router's strategy — mirrors Rust {@code strategy_bytes}. */
  private byte[] strategyBytes(Coordinates key) {
    if (strategy == Strategy.PARTITION && key.getPartitionCount() > 0) {
      // Partition routing hashes the partition fields only (role 0, no identifier),
      // matching Rust's `CompositeKey::new(key.partition, vec![]).encode()`.
      return encode(key.getPartitionList(), List.of());
    }
    return encode(key.getPartitionList(), key.getIdentifierList());
  }

  /**
   * The <b>bucket</b> ({@code [0, NUM_BUCKETS)}) a key hashes to under this router's strategy
   * (task-77) — independent of placement. Mirrors Rust {@code ShardRouter::bucket}.
   */
  public int bucket(Coordinates key) {
    return (int) Long.remainderUnsigned(fnv1a(strategyBytes(key)), NUM_BUCKETS);
  }

  /** The shard ordinal ({@code [0, shards)}) that owns {@code key}. */
  public int route(Coordinates key) {
    if (bucketOwners != null) {
      return bucketOwners[bucket(key)];
    }
    if (shards <= 1) {
      return 0;
    }
    // Unsigned remainder: the Rust side computes `fnv1a(..) % shards` on a u64.
    return (int) Long.remainderUnsigned(fnv1a(strategyBytes(key)), shards);
  }

  /**
   * Canonical, type-tagged key encoding — {@code partition[] ++ identifier[]}, each field as
   * {@code role · len(name) · name · type-tag · len(value) · value} with little-endian length
   * prefixes. Mirrors {@code CompositeKey::encode} in {@code growlerdb-core}.
   */
  static byte[] encode(List<Field> partition, List<Field> identifier) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    encodeFields(out, (byte) 0, partition);
    encodeFields(out, (byte) 1, identifier);
    return out.toByteArray();
  }

  private static void encodeFields(ByteArrayOutputStream out, byte role, List<Field> fields) {
    for (Field f : fields) {
      out.write(role);
      pushBytes(out, f.getName().getBytes(StandardCharsets.UTF_8));
      pushValue(out, f.getValue());
    }
  }

  /** {@code u32} little-endian length prefix, then the bytes. */
  private static void pushBytes(ByteArrayOutputStream out, byte[] bytes) {
    int n = bytes.length;
    out.write(n & 0xff);
    out.write((n >>> 8) & 0xff);
    out.write((n >>> 16) & 0xff);
    out.write((n >>> 24) & 0xff);
    out.write(bytes, 0, bytes.length);
  }

  /** Type tag, then the value's little-endian bytes — mirrors Rust {@code push_value}. */
  private static void pushValue(ByteArrayOutputStream out, Value v) {
    switch (v.getKindCase()) {
      case STR -> {
        out.write(1);
        pushBytes(out, v.getStr().getBytes(StandardCharsets.UTF_8));
      }
      case INT -> {
        out.write(2);
        pushBytes(out, longLe(v.getInt()));
      }
      case FLOAT -> {
        out.write(3);
        pushBytes(out, longLe(Double.doubleToLongBits(v.getFloat())));
      }
      case BOOL -> {
        out.write(4);
        pushBytes(out, new byte[] {(byte) (v.getBool() ? 1 : 0)});
      }
      case TS_MICROS -> {
        // Canonical epoch micros (task-184) — same 8-byte LE shape as INT, under tag 5,
        // mirroring Rust's `Value::Ts` arm in `push_value`.
        out.write(5);
        pushBytes(out, longLe(v.getTsMicros()));
      }
      case KIND_NOT_SET -> throw new IllegalArgumentException("key field has no value set");
    }
  }

  private static byte[] longLe(long x) {
    byte[] b = new byte[8];
    for (int i = 0; i < 8; i++) {
      b[i] = (byte) (x >>> (8 * i));
    }
    return b;
  }

  /** FNV-1a 64-bit — the same stable, dependency-free hash the Rust router uses. */
  static long fnv1a(byte[] bytes) {
    long h = 0xcbf29ce484222325L;
    for (byte b : bytes) {
      h ^= (b & 0xffL);
      h *= 0x100000001b3L; // wraps modulo 2^64, exactly as Rust's wrapping_mul
    }
    return h;
  }
}
