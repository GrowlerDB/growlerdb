package io.growlerdb.connector;

import io.growlerdb.proto.v1.WindowingConfig;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

/**
 * {@code spark-submit} entrypoint for the ingestion connector (task-11): a Spark
 * Structured Streaming job that drives {@link ConnectorJob} — changelog read →
 * {@code DocOp} mapping → Write gRPC to a GrowlerDB Node ({@code growlerdb serve}).
 *
 * <p>The catalog is configured by the submitter via {@code --conf
 * spark.sql.catalog.<name>.*} (Hadoop catalog for local; Polaris REST for the dev
 * stack), so this app is catalog-agnostic. Run modes:
 *
 * <ul>
 *   <li><b>default (one batch):</b> read the window since {@code --start} to the
 *       current snapshot, commit it, print the new checkpoint, exit. This is the
 *       fully-verified path (the cross-process integration test).
 *   <li><b>{@code --stream}:</b> a {@code foreachBatch} loop that re-runs the batch
 *       on every new snapshot, resuming from the in-memory cursor.
 * </ul>
 *
 * <p><b>Resume (task-16):</b> unless {@code --start} overrides it, the start
 * checkpoint is read from the Node via {@link WriteClient#checkpointSnapshotId()} —
 * the position it has durably committed — so a restart resumes exactly-once (atomic
 * write+checkpoint commit; {@code batch_id} dedups a boundary re-read).
 *
 * <p><b>Deferred (not silently skipped):</b> Spark-on-K8s {@code spark-submit}
 * packaging and streaming checkpoint/restart <i>resumability at scale</i> are
 * verified only in a real cluster, not in-repo.
 */
public final class ConnectorApp {

  /** Backoff before restarting the streaming query in-process after a batch failure (task-144). */
  static final int STREAM_RESTART_BACKOFF_SECS = 5;

  public static void main(String[] args) throws Exception {
    // task-125: re-resolve Node DNS promptly so the write client picks up a restarted shard pod's new
    // IP within seconds (in-place reconnect) instead of re-dialing the dead cached IP until the retry
    // budget forces a pod restart (task-124's worst case). The JDK caches successful lookups — often
    // effectively forever — so a crashed-then-returned pod keeps resolving to its dead address. Cap
    // the TTL. Must run before any hostname is resolved (i.e. before Spark/gRPC start), so it's the
    // very first thing main does. Overridable via GROWLERDB_DNS_TTL_SECONDS for ops tuning.
    capDnsCacheTtl();
    // Start the connector metrics endpoint (task-194 AC6) — a no-op unless GROWLERDB_METRICS_PORT is
    // set, so the ingest-side signals survive log rotation without binding a port in local runs.
    ConnectorMetrics.startServer();
    Map<String, String> opts = parse(args);
    String catalog = opts.getOrDefault("catalog", "demo");
    String table = require(opts, "table");
    List<String> identifier = csv(opts.getOrDefault("identifier", "id"));
    List<String> fields = csv(opts.getOrDefault("fields", "id,body"));
    List<String> partition = csv(opts.getOrDefault("partition", ""));
    Long start = opts.containsKey("start") ? Long.parseLong(opts.get("start")) : null;
    boolean stream = opts.containsKey("stream");
    // task-113: cap each commit's changelog rows so a large catch-up window is committed in bounded
    // sub-batches instead of one oversized Write. 0/absent → the ConnectorJob default.
    long maxCommitRows =
        opts.containsKey("max-commit-rows") ? Long.parseLong(opts.get("max-commit-rows")) : 0;

    // Parallel connector set (task-196): `--workers W` + `--worker-id i` (arg wins over the
    // GROWLERDB_WORKER_ID env — the StatefulSet pod index). Worker i owns shards {s : s % W == i}
    // and writes ONLY those; its resume is its own group's checkpoint min. Flags absent ⇒ the
    // classic single-connector path, unchanged.
    Integer workers = opts.containsKey("workers") ? Integer.parseInt(opts.get("workers")) : null;
    Integer workerId = workerId(opts);
    if ((workers == null) != (workerId == null)) {
      throw new IllegalArgumentException(
          "--workers and --worker-id (or GROWLERDB_WORKER_ID) must be given together");
    }
    if (workers != null && start != null) {
      // A global --start override cannot be sound per worker: each group resumes from its OWN
      // shards' committed checkpoints, and a forced common start would gap groups that are ahead.
      throw new IllegalArgumentException(
          "--start cannot be combined with --workers: each worker resumes from its shard group's checkpoints");
    }

    // Target one Node (`--node host:port`) or a sharded cluster (`--nodes h1:p1,h2:p2,…`).
    List<String> nodes = csv(opts.getOrDefault("nodes", opts.getOrDefault("node", "127.0.0.1:50051")));

    // Routing source of truth (task-69): when a `--control-plane host:port` (+ `--index`) is given,
    // fetch the shard count and strategy from the registry — the same source the Gateway reads —
    // and fail fast if the local config disagrees, so writes can't land where reads never look.
    // Without it, fall back to deriving the strategy from `--partition` (the legacy/dev path).
    ShardRouter.Strategy routing;
    // Virtual-bucket map from the registry (task-77): when present, the connector routes
    // `key → bucket → shard` through the same map the Gateway reads, so writes land where reads
    // look. Empty/absent ⇒ legacy `fnv % shards`.
    int[] bucketOwners = null;
    // Windowed index (task-219): when the registry reports windowing, the connector routes each row
    // to its TIME WINDOW's owning node (resolved live from the control plane) rather than by key-hash,
    // so it keeps a long-lived CP client for the run instead of closing it after GetIndex.
    WindowingConfig windowing = null;
    ControlPlaneClient windowedCp = null;
    String controlPlane = opts.getOrDefault("control-plane", "");
    if (!controlPlane.isEmpty()) {
      String index = require(opts, "index");
      String[] hp = controlPlane.split(":", 2);
      if (hp.length != 2) {
        throw new IllegalArgumentException("--control-plane must be host:port, got `" + controlPlane + "`");
      }
      ControlPlaneClient cp = new ControlPlaneClient(hp[0].trim(), Integer.parseInt(hp[1].trim()));
      boolean keepCp = false;
      try {
        var entry = cp.getIndex(index);
        if (entry.hasWindowing()) {
          windowing = entry.getWindowing();
          windowedCp = cp;
          keepCp = true;
          routing = ShardRouter.Strategy.HASH; // unused for windowed (routes by window, not key)
          System.out.printf(
              "windowed index %s: window field=%s granularity=%s%n",
              index, windowing.getField(), windowing.getGranularity());
        } else {
          routing = resolveRouting(entry.getShardCount(), strategyOf(entry.getRouting()), nodes.size(), partition);
          if (entry.getBucketOwnersCount() > 0) {
            bucketOwners = entry.getBucketOwnersList().stream().mapToInt(Integer::intValue).toArray();
          }
          System.out.printf(
              "routing from registry: index=%s shards=%d strategy=%s buckets=%s%n",
              index, entry.getShardCount(), routing, bucketOwners != null ? "yes" : "no");
        }
      } finally {
        if (!keepCp) {
          cp.close();
        }
      }
    } else {
      routing = routingFor(partition);
    }
    if (windowing != null && workers != null) {
      throw new IllegalArgumentException(
          "--workers is not supported for a windowed index (task-219): windows are routed by time,"
              + " not hash shard groups");
    }

    IndexMapping mapping = new IndexMapping(partition, identifier, fields);
    ConnectorJob job =
        new ConnectorJob(catalog, table, mapping, identifier, java.util.Set.of(), maxCommitRows);

    java.util.SortedSet<Integer> owned = null;
    ShardRouter router = null;
    if (workers != null) {
      router =
          (bucketOwners != null && bucketOwners.length > 0)
              ? ShardRouter.bucketed(routing, bucketOwners)
              : new ShardRouter(nodes.size(), routing);
      owned = ShardGroup.owned(workerId, workers, nodes.size());
      if (owned.isEmpty()) {
        // Fail fast: a CrashLooping extra pod is a visible misconfiguration; a silently idle
        // worker is not.
        throw new IllegalArgumentException(
            "worker "
                + workerId
                + " of "
                + workers
                + " owns no shards over "
                + nodes.size()
                + " — reduce the set's replicas to at most the shard count");
      }
      job = job.ownedBy(router, owned);
      System.out.printf("connector set: worker %d/%d owns shards %s%n", workerId, workers, owned);
    }

    SparkSession spark =
        SparkSession.builder()
            .appName("growlerdb-connector")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .getOrCreate();

    SnapshotLineage lineage = SnapshotLineage.forTable(spark, catalog + "." + table);
    final ControlPlaneClient cpToClose = windowedCp;
    try (BatchWriter client =
        windowing != null
            ? new WindowedWriteClient(require(opts, "index"), windowedCp, windowing, lineage)
            : owned != null
                ? new ShardGroupWriteClient(nodes, router, lineage, owned)
                : writerFor(nodes, routing, bucketOwners, lineage)) {
      // Resume exactly-once: unless an explicit --start override is given, pick up
      // from the checkpoint the Node has durably committed (task-16). null = the
      // shard is empty, so read the changelog from the beginning.
      Long resumeFrom = (start != null) ? start : client.checkpointSnapshotId();
      System.out.printf(
          "resuming from %s%n", resumeFrom == null ? "the start (no checkpoint)" : resumeFrom);
      if (stream) {
        // task-144: a write that exhausts its retry budget (e.g. ALL Node pods mid-roll, dialing
        // stale IPs) fails the micro-batch → the streaming query fails. Restart it IN-PROCESS —
        // resuming from the Node's durable checkpoint (exactly-once rests there, not on Spark's
        // offset) — rather than letting awaitTermination() throw → JVM exit(1) → CrashLoopBackOff.
        // So a full node roll drains lag and recovers with the connector staying up (RESTARTS flat).
        int restarts = 0;
        while (true) {
          try {
            runStream(spark, job, client, resumeFrom).awaitTermination();
            break; // graceful stop (SIGTERM) — the query completed, exit the loop
          } catch (StreamingQueryException e) {
            restarts++;
            ConnectorMetrics.recordStreamRestart(); // survives log rotation (task-194 AC6)
            System.err.printf(
                "connector: stream failed (%s); restart #%d in %ds — resuming from the Node checkpoint%n",
                e.getMessage(), restarts, STREAM_RESTART_BACKOFF_SECS);
            Thread.sleep(STREAM_RESTART_BACKOFF_SECS * 1000L);
            try {
              resumeFrom = client.checkpointSnapshotId(); // latest committed; retries the Node
            } catch (RuntimeException stillDown) {
              // Nodes still unreachable — keep the last resume; the changelog replay is idempotent
              // (the Node dedups by committed checkpoint), so re-reading from it is a safe no-op.
            }
          }
        }
      } else {
        ConnectorJob.Result r = job.runOnce(spark, resumeFrom, client);
        if (r.wrote) {
          System.out.printf(
              "committed %d op(s) → index snapshot %d; checkpoint=%d%n",
              r.opCount, r.committedSnapshot, r.checkpointSnapshotId);
        } else {
          System.out.println("nothing to commit (table unborn or already caught up)");
        }
      }
    } finally {
      spark.stop();
      if (cpToClose != null) {
        cpToClose.close(); // the windowed writer borrows this CP client for ResolveWindowOwner
      }
    }
  }

  /**
   * Drive {@link ConnectorJob#runOnce} once per new snapshot via {@code foreachBatch}.
   * The Iceberg stream is only a <b>trigger</b> — the change set is re-derived from
   * the changelog procedure each time — so non-append snapshots are skipped rather
   * than failing the stream. The cursor is seeded from the Node's durable checkpoint
   * ({@code start}) and advances in memory per trigger; exactly-once across a restart
   * rests on the Node's atomic write+checkpoint commit, not Spark's stream offset.
   */
  static StreamingQuery runStream(
      SparkSession spark, ConnectorJob job, BatchWriter client, Long start)
      throws java.util.concurrent.TimeoutException {
    AtomicReference<Long> cursor = new AtomicReference<>(start);
    // A heartbeat trigger only — the change set is re-derived from the changelog in runOnce each
    // batch, so the trigger source's content is irrelevant. We use Spark's built-in `rate` source
    // (not the Iceberg streaming source) because the Iceberg source writes its offset log through the
    // table's FileIO — with S3FileIO that rejects the local `file:` checkpoint ("Invalid S3 URI").
    // The rate source checkpoints on the local FS, so the connector streams against any object store.
    Dataset<Row> trigger = spark.readStream().format("rate").option("rowsPerSecond", 1).load();
    // Spark Structured Streaming needs a checkpoint location for its source-offset log. Pin it to a
    // LOCAL path (file://) so Spark uses the LocalFileSystem for it — not the Iceberg table's
    // S3FileIO, which rejects `file:` paths ("Invalid S3 URI"). This is only Spark's stream cursor;
    // GrowlerDB's exactly-once rests on the Node's atomic write+checkpoint commit (so losing this on
    // a restart just re-reads the changelog from the Node's durable checkpoint — a no-op replay).
    String checkpoint =
        "file://" + System.getProperty("java.io.tmpdir", "/tmp") + "/growlerdb-connector-ckpt";
    return trigger
        .writeStream()
        .option("checkpointLocation", checkpoint)
        .trigger(Trigger.ProcessingTime("5 seconds"))
        .foreachBatch(
            (Dataset<Row> batchDf, Long batchId) -> {
              ConnectorJob.Result r = job.runOnce(spark, cursor.get(), client);
              if (r.wrote) {
                cursor.set(r.checkpointSnapshotId);
                System.out.printf(
                    "[trigger %d] committed %d op(s) → snapshot %d%n",
                    batchId, r.opCount, r.committedSnapshot);
              }
            })
        .start();
  }

  /**
   * One Node → a direct {@link WriteClient}; several → a {@link ShardedWriteClient} that routes
   * each op to its owning shard with {@code routing} (partition when the key is partitioned,
   * else hash — the same rule the Gateway derives from the index definition).
   */
  /**
   * The routing strategy for an index with these {@code partitionFields}: partition routing when
   * the key is partitioned (co-locate a partition on a shard), else hash. Mirrors Rust
   * {@code ResolvedIndex::routing_strategy}, so the connector places writes on the same shard the
   * Gateway reads them from. (The connector must be configured with the index's partition fields;
   * it does not re-derive them from the source schema.)
   */
  /** {@code --worker-id} arg, else the {@code GROWLERDB_WORKER_ID} env (the pod index), else null. */
  static Integer workerId(Map<String, String> opts) {
    if (opts.containsKey("worker-id")) {
      return Integer.parseInt(opts.get("worker-id"));
    }
    String env = System.getenv("GROWLERDB_WORKER_ID");
    return (env == null || env.isBlank()) ? null : Integer.parseInt(env.trim());
  }

  static ShardRouter.Strategy routingFor(List<String> partitionFields) {
    return partitionFields.isEmpty() ? ShardRouter.Strategy.HASH : ShardRouter.Strategy.PARTITION;
  }

  /** Map the wire {@code RoutingStrategy} (from the registry) to the connector's {@link ShardRouter.Strategy}. */
  static ShardRouter.Strategy strategyOf(io.growlerdb.proto.v1.RoutingStrategy routing) {
    return routing == io.growlerdb.proto.v1.RoutingStrategy.ROUTING_PARTITION
        ? ShardRouter.Strategy.PARTITION
        : ShardRouter.Strategy.HASH;
  }

  /**
   * Validate the connector's local config against the registry's routing config (task-69) and
   * return the authoritative strategy. Fails fast — rather than silently misplacing every doc —
   * when:
   *
   * <ul>
   *   <li>the endpoint count ({@code --nodes}) ≠ the registry shard count (writes by {@code %n}
   *       but reads by {@code %m}), or
   *   <li>the strategy implied by {@code --partition} disagrees with the registry's (a partitioned
   *       index routed by hash, or vice versa).
   * </ul>
   *
   * The registry is authoritative; the {@code --partition} check only guards against a contradictory
   * local config (the connector still needs the partition fields to build keys).
   */
  static ShardRouter.Strategy resolveRouting(
      int registryShardCount,
      ShardRouter.Strategy registryStrategy,
      int endpointCount,
      List<String> partitionFields) {
    if (endpointCount != registryShardCount) {
      throw new IllegalStateException(
          "routing config mismatch: "
              + endpointCount
              + " --nodes endpoint(s) but the registry has "
              + registryShardCount
              + " shard(s) — writes would land where reads never look; align --nodes with the index shard map");
    }
    ShardRouter.Strategy local = routingFor(partitionFields);
    if (local != registryStrategy) {
      throw new IllegalStateException(
          "routing strategy mismatch: --partition implies "
              + local
              + " but the registry resolves the index to "
              + registryStrategy
              + " — fix --partition to match the index definition");
    }
    return registryStrategy;
  }

  static BatchWriter writerFor(List<String> nodes, ShardRouter.Strategy routing) {
    return writerFor(nodes, routing, null);
  }

  /**
   * One Node → a direct {@link WriteClient}; several → a {@link ShardedWriteClient}. When
   * {@code bucketOwners} is non-empty (task-77), the sharded writer routes through that bucket map
   * (matching the Gateway); otherwise legacy {@code fnv % shards}. A single node always routes to
   * shard 0, so the bucket map is irrelevant there.
   */
  static BatchWriter writerFor(List<String> nodes, ShardRouter.Strategy routing, int[] bucketOwners) {
    return writerFor(nodes, routing, bucketOwners, SnapshotLineage.none());
  }

  /**
   * As above, with the source table's {@link SnapshotLineage} so the sharded resume-min orders
   * diverged shard checkpoints by sequence number instead of the random snapshot id (task-205).
   */
  static BatchWriter writerFor(
      List<String> nodes, ShardRouter.Strategy routing, int[] bucketOwners, SnapshotLineage lineage) {
    if (nodes.size() == 1) {
      String[] hp = nodes.get(0).split(":", 2);
      return new WriteClient(hp[0].trim(), Integer.parseInt(hp[1].trim()));
    }
    if (bucketOwners != null && bucketOwners.length > 0) {
      return new ShardedWriteClient(nodes, ShardRouter.bucketed(routing, bucketOwners), lineage);
    }
    return new ShardedWriteClient(nodes, new ShardRouter(nodes.size(), routing), lineage);
  }

  /**
   * Cap the JDK's positive DNS cache so a restarted Node's new pod IP is picked up within seconds
   * (task-125). {@code networkaddress.cache.ttl} is a <i>security</i> property, not a {@code -D}
   * system property, so set it programmatically — and early, before the cache policy is read on the
   * first lookup. Default 3s; {@code GROWLERDB_DNS_TTL_SECONDS} overrides.
   */
  private static void capDnsCacheTtl() {
    String ttl = System.getenv("GROWLERDB_DNS_TTL_SECONDS");
    if (ttl == null || ttl.isBlank()) {
      ttl = "3";
    }
    java.security.Security.setProperty("networkaddress.cache.ttl", ttl.trim());
    java.security.Security.setProperty("networkaddress.cache.negative.ttl", "1");
  }

  // --- tiny arg parsing: `--key value` and bare `--flag` -----------------------

  private static Map<String, String> parse(String[] args) {
    Map<String, String> opts = new HashMap<>();
    for (int i = 0; i < args.length; i++) {
      if (!args[i].startsWith("--")) {
        continue;
      }
      String key = args[i].substring(2);
      if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
        opts.put(key, args[++i]);
      } else {
        opts.put(key, "");
      }
    }
    return opts;
  }

  private static String require(Map<String, String> opts, String key) {
    String v = opts.get(key);
    if (v == null || v.isEmpty()) {
      throw new IllegalArgumentException("missing required --" + key);
    }
    return v;
  }

  private static List<String> csv(String s) {
    if (s == null || s.isBlank()) {
      return List.of();
    }
    return Arrays.stream(s.split(",")).map(String::trim).filter(x -> !x.isEmpty()).toList();
  }

  private ConnectorApp() {}
}
