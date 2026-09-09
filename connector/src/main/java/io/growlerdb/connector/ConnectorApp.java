package io.growlerdb.connector;

import io.growlerdb.proto.v1.GetIndexResponse;
import io.growlerdb.proto.v1.ShardStatus;
import io.growlerdb.proto.v1.WindowingConfig;
import java.util.ArrayList;
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
 * {@code spark-submit} entrypoint for the ingestion connector: a Spark
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
 * <p><b>Resume:</b> unless {@code --start} overrides it, the start
 * checkpoint is read from the Node via {@link WriteClient#checkpointSnapshotId()} —
 * the position it has durably committed — so a restart resumes exactly-once (atomic
 * write+checkpoint commit; {@code batch_id} dedups a boundary re-read).
 *
 * <p><b>Deferred (not silently skipped):</b> Spark-on-K8s {@code spark-submit}
 * packaging and streaming checkpoint/restart <i>resumability at scale</i> are
 * verified only in a real cluster, not in-repo.
 */
public final class ConnectorApp {

  /** Backoff before restarting the streaming query in-process after a batch failure. */
  static final int STREAM_RESTART_BACKOFF_SECS = 5;

  public static void main(String[] args) throws Exception {
    // Must run before any hostname is resolved (before Spark/gRPC start) — see capDnsCacheTtl.
    capDnsCacheTtl();
    // Start the connector metrics endpoint — a no-op unless GROWLERDB_METRICS_PORT is set, so the
    // ingest-side signals survive log rotation without binding a port in local runs.
    ConnectorMetrics.startServer();
    Map<String, String> opts = parse(args);
    String catalog = opts.getOrDefault("catalog", "demo");
    String table = require(opts, "table");
    List<String> identifier = csv(opts.getOrDefault("identifier", "id"));
    List<String> fields = csv(opts.getOrDefault("fields", "id,body"));
    List<String> partition = csv(opts.getOrDefault("partition", ""));
    Long start = opts.containsKey("start") ? Long.parseLong(opts.get("start")) : null;
    boolean stream = opts.containsKey("stream");
    // Cap each commit's changelog rows so a large catch-up window is committed in bounded sub-batches
    // instead of one oversized Write. 0/absent → the ConnectorJob default.
    long maxCommitRows =
        opts.containsKey("max-commit-rows") ? Long.parseLong(opts.get("max-commit-rows")) : 0;

    // Parallel connector set: `--workers W` + `--worker-id i` (arg wins over the GROWLERDB_WORKER_ID
    // env — the StatefulSet pod index). Worker i owns shards {s : s % W == i} and writes ONLY those;
    // its resume is its own group's checkpoint min. Flags absent ⇒ the single-connector path.
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
    // Did the operator pin endpoints explicitly? If not, a hash index with `--control-plane` sources
    // each shard's owning node from the registry (below) instead of this default.
    boolean explicitNodes = opts.containsKey("nodes") || opts.containsKey("node");
    // Tag every sharded sub-batch with the index, so a pool node serving many indexes can dispatch it
    // by `(index, shard)`; empty (no `--index`) ⇒ the node's sole served index ignores it.
    String indexTag = opts.getOrDefault("index", "");

    // Routing source of truth: with `--control-plane` (+ `--index`), fetch shard count and strategy
    // from the registry — the same source the Gateway reads — and fail fast if local config disagrees,
    // so writes can't land where reads never look. Without it, derive the strategy from `--partition`.
    ShardRouter.Strategy routing;
    // Virtual-bucket map from the registry: routes `key → bucket → shard` through the same map the
    // Gateway reads, so writes land where reads look. Empty/absent ⇒ `fnv % shards`.
    int[] bucketOwners = null;
    // Windowed index: the connector routes each row to its time window's owning node (resolved live
    // from the control plane) rather than by key-hash.
    WindowingConfig windowing = null;
    // The long-lived CP client — kept open whenever placement is CP-driven, so a stream restart can
    // RE-resolve placement instead of pinning the startup snapshot. Null when placement is static.
    ControlPlaneClient placementCp = null;
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
          placementCp = cp;
          keepCp = true;
          routing = ShardRouter.Strategy.HASH; // unused for windowed (routes by window, not key)
          System.out.printf(
              "windowed index %s: window field=%s granularity=%s%n",
              index, windowing.getField(), windowing.getGranularity());
        } else {
          if (entry.getBucketOwnersCount() > 0) {
            bucketOwners = entry.getBucketOwnersList().stream().mapToInt(Integer::intValue).toArray();
          }
          // CP-driven placement: unless `--nodes` pins endpoints, source each shard's owning node from
          // the registry's shard map (re-read on every stream restart — see the writer factory) so
          // writes follow the control plane and can't drift from where reads look.
          if (!explicitNodes) {
            nodes = shardEndpointsFromCp(entry);
            placementCp = cp;
            keepCp = true;
          }
          routing = resolveRouting(entry.getShardCount(), strategyOf(entry.getRouting()), nodes.size(), partition);
          System.out.printf(
              "routing from registry: index=%s shards=%d strategy=%s buckets=%s endpoints=%s%n",
              index,
              entry.getShardCount(),
              routing,
              bucketOwners != null ? "yes" : "no",
              explicitNodes ? "static" : "control-plane");
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
          "--workers is not supported for a windowed index: windows are routed by time,"
              + " not hash shard groups");
    }

    IndexMapping mapping = new IndexMapping(partition, identifier, fields);
    ConnectorJob job =
        new ConnectorJob(catalog, table, mapping, identifier, java.util.Set.of(), maxCommitRows);

    // Variant extraction (D47/D48): `--variant-spec <json>` declares the variant column, flatten
    // flags, discriminator, and shapes; each row's variant is walked into flatten leaves +
    // discriminator-selected shape values. Absent ⇒ none. (`--fields` must list the shaped paths.)
    if (opts.containsKey("variant-spec")) {
      VariantSpec spec = VariantSpec.fromJson(opts.get("variant-spec"));
      job = job.withVariant(new VariantExtractor(spec));
      System.out.printf(
          "variant: extracting column '%s' (discriminator '%s', %d shapes)%n",
          spec.column, spec.discriminator, spec.shapes.size());
    }

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

    SparkSession.Builder builder =
        SparkSession.builder()
            .appName("growlerdb-connector")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");
    // Apply S3 creds before the catalog is lazily built on first table access (next stmt) — D55.
    s3CatalogConf(catalog, System.getenv()).forEach(builder::config);
    SparkSession spark = builder.getOrCreate();

    SnapshotLineage lineage = SnapshotLineage.forTable(spark, catalog + "." + table);
    // The writer FACTORY, not a writer: every stream restart re-invokes it, so a CP-driven run
    // re-resolves the CURRENT shard→node placement instead of pinning the startup snapshot (else a
    // re-placement leaves restarts hammering the deposed endpoint). Static placement rebuilds to the
    // same endpoints with fresh channels.
    final ControlPlaneClient cp = placementCp;
    final WindowingConfig windowCfg = windowing;
    final List<String> staticNodes = nodes;
    final ShardRouter.Strategy strategy = routing;
    final int[] buckets = bucketOwners;
    final ShardRouter groupRouter = router;
    final java.util.SortedSet<Integer> ownedShards = owned;
    java.util.function.Supplier<BatchWriter> writerFactory;
    if (windowCfg != null) {
      // A fresh windowed writer also starts with an EMPTY window→owner cache — the in-write
      // invalidation (WindowedWriteClient) already heals a single stale window in place.
      writerFactory = () -> new WindowedWriteClient(indexTag, cp, windowCfg, lineage);
    } else if (cp != null) {
      writerFactory =
          cpResolvedWriterFactory(cp, indexTag, strategy, buckets, lineage, groupRouter, ownedShards);
    } else {
      writerFactory =
          () ->
              ownedShards != null
                  ? new ShardGroupWriteClient(staticNodes, groupRouter, lineage, ownedShards, indexTag)
                  : writerFor(staticNodes, strategy, buckets, lineage, indexTag);
    }

    try {
      // Manual writer lifecycle (not try-with-resources): the restart loop REPLACES the writer after a
      // re-resolution, so the inner finally closes whichever writer is CURRENT when the run ends.
      BatchWriter client = writerFactory.get();
      try {
        // Resume exactly-once: unless --start overrides, pick up from the Node's durably-committed
        // checkpoint. null = empty shard, so read the changelog from the beginning.
        Long resumeFrom = (start != null) ? start : client.checkpointSnapshotId();
        System.out.printf(
            "resuming from %s%n", resumeFrom == null ? "the start (no checkpoint)" : resumeFrom);
        if (stream) {
          // A failed micro-batch fails the streaming query; restart it IN-PROCESS, resuming from the
          // Node's durable checkpoint (exactly-once rests there, not on Spark's offset), rather than
          // letting awaitTermination() throw → exit(1) → CrashLoopBackOff. So a full node roll drains
          // lag with the connector staying up (RESTARTS flat).
          int restarts = 0;
          while (true) {
            try {
              runStream(spark, job, client, resumeFrom).awaitTermination();
              break; // graceful stop (SIGTERM) — the query completed, exit the loop
            } catch (StreamingQueryException e) {
              restarts++;
              ConnectorMetrics.recordStreamRestart(); // survives log rotation
              System.err.printf(
                  "connector: stream failed (%s); restart #%d in %ds — resuming from the Node checkpoint%n",
                  e.getMessage(), restarts, STREAM_RESTART_BACKOFF_SECS);
              Thread.sleep(STREAM_RESTART_BACKOFF_SECS * 1000L);
              // Re-resolve placement BEFORE resuming: the failure may be a CP re-placement, and only a
              // rebuilt writer follows the move. Exactly-once holds: the new writer re-reads its resume
              // point from the Nodes' durable checkpoints, and idempotent batch ids dedup any replay.
              client = rebuildWriter(client, writerFactory);
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
        client.close();
      }
    } finally {
      spark.stop();
      if (cp != null) {
        cp.close(); // the CP-driven writer factory borrows this client across restarts
      }
    }
  }

  /**
   * A writer factory over <b>CP-resolved hash placement</b>: each call re-reads the index's CURRENT
   * shard→node map from the control plane ({@link #shardEndpointsFromCp}) and connects a fresh writer,
   * so the restart loop follows a re-placement. Routing stays fixed for the process — a re-placement
   * moves a shard's <i>node</i>, never its ordinal; a reshard still requires a connector restart.
   */
  static java.util.function.Supplier<BatchWriter> cpResolvedWriterFactory(
      ControlPlaneClient cp,
      String index,
      ShardRouter.Strategy strategy,
      int[] bucketOwners,
      SnapshotLineage lineage,
      ShardRouter groupRouter,
      java.util.SortedSet<Integer> owned) {
    return () -> {
      List<String> endpoints = shardEndpointsFromCp(cp.getIndex(index));
      return owned != null
          ? new ShardGroupWriteClient(endpoints, groupRouter, lineage, owned, index)
          : writerFor(endpoints, strategy, bucketOwners, lineage, index);
    };
  }

  /**
   * Swap {@code current} for a freshly-built writer (closing the old one), or <b>keep it</b> when
   * the rebuild itself fails — e.g. the CP is unreachable mid-failover — so the restart loop
   * degrades to the old retry-in-place behavior and re-resolves again on the next restart.
   */
  static BatchWriter rebuildWriter(
      BatchWriter current, java.util.function.Supplier<BatchWriter> factory) {
    BatchWriter fresh;
    try {
      fresh = factory.get();
    } catch (RuntimeException resolveDown) {
      System.err.printf(
          "connector: placement re-resolution failed (%s) — keeping the current writer; the next"
              + " restart re-resolves%n",
          resolveDown.getMessage());
      return current;
    }
    closeQuietly(current);
    return fresh;
  }

  private static void closeQuietly(BatchWriter writer) {
    try {
      writer.close();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Drive {@link ConnectorJob#runOnce} once per new snapshot via {@code foreachBatch}. The Iceberg
   * stream is only a <b>trigger</b> (the change set is re-derived from the changelog each time), so
   * non-append snapshots are skipped rather than failing the stream. The cursor advances in memory
   * per trigger; exactly-once across a restart rests on the Node's checkpoint, not Spark's offset.
   */
  static StreamingQuery runStream(
      SparkSession spark, ConnectorJob job, BatchWriter client, Long start)
      throws java.util.concurrent.TimeoutException {
    AtomicReference<Long> cursor = new AtomicReference<>(start);
    // A heartbeat trigger only — the change set is re-derived from the changelog in runOnce each
    // batch, so trigger content is irrelevant. Use the `rate` source, not the Iceberg streaming
    // source: the latter writes its offset log through the table's FileIO, which S3FileIO rejects for
    // a local `file:` checkpoint ("Invalid S3 URI"); rate checkpoints on the local FS.
    Dataset<Row> trigger = spark.readStream().format("rate").option("rowsPerSecond", 1).load();
    // Spark's source-offset checkpoint — pinned LOCAL (file://) so it uses LocalFileSystem, not the
    // table's S3FileIO (which rejects `file:` paths). Only Spark's cursor; GrowlerDB exactly-once
    // rests on the Node's checkpoint, so losing this on a restart is a no-op changelog replay.
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
   * Validate the connector's local config against the registry's routing config and
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
   * The per-ordinal owning-node endpoints of a hash-sharded index, read from the registry's shard map
   * ({@code GetIndex.shard_status}) — the same placement the Gateway routes reads to. Returned in
   * ordinal order, each stripped to {@code host:port}. Fails fast if a shard has no live primary yet
   * rather than silently dropping its writes.
   */
  static List<String> shardEndpointsFromCp(GetIndexResponse entry) {
    int count = entry.getShardCount();
    String[] byOrdinal = new String[count];
    for (ShardStatus s : entry.getShardStatusList()) {
      if (s.getWindow() != 0) {
        continue; // ordinal shards only (a windowed index never reaches here)
      }
      int ordinal = s.getOrdinal();
      if (ordinal >= 0 && ordinal < count && !s.getPrimary().isEmpty()) {
        String endpoint = s.getPrimary().replaceFirst("^https?://", "");
        // One ShardStatus per ordinal is the registry contract; a duplicate means the shard map is
        // ambiguous about who owns the shard's writes. Fail loudly rather than silently letting the
        // LAST entry win — guessing wrong writes where reads never look.
        if (byOrdinal[ordinal] != null) {
          throw new IllegalStateException(
              "registry shard map lists shard "
                  + ordinal
                  + " more than once (`"
                  + byOrdinal[ordinal]
                  + "` and `"
                  + endpoint
                  + "`) — ambiguous placement; refusing to pick a primary");
        }
        byOrdinal[ordinal] = endpoint;
      }
    }
    List<String> endpoints = new ArrayList<>(count);
    for (int ordinal = 0; ordinal < count; ordinal++) {
      if (byOrdinal[ordinal] == null) {
        throw new IllegalStateException(
            "index has no live primary for shard "
                + ordinal
                + " of "
                + count
                + " yet — ensure every shard's serve node is registered with the control plane before"
                + " starting the connector (or pass --nodes to pin endpoints)");
      }
      endpoints.add(byOrdinal[ordinal]);
    }
    return endpoints;
  }

  /**
   * One Node → a direct {@link WriteClient}; several → a {@link ShardedWriteClient}. When
   * {@code bucketOwners} is non-empty, the sharded writer routes through that bucket map
   * (matching the Gateway); otherwise {@code fnv % shards}. A single node always routes to
   * shard 0, so the bucket map is irrelevant there.
   */
  static BatchWriter writerFor(List<String> nodes, ShardRouter.Strategy routing, int[] bucketOwners) {
    return writerFor(nodes, routing, bucketOwners, SnapshotLineage.none());
  }

  /** As below, untagged (empty index — a single-index sharded deployment). */
  static BatchWriter writerFor(
      List<String> nodes, ShardRouter.Strategy routing, int[] bucketOwners, SnapshotLineage lineage) {
    return writerFor(nodes, routing, bucketOwners, lineage, "");
  }

  /**
   * As above, with the source table's {@link SnapshotLineage} so the sharded resume-min orders
   * diverged shard checkpoints by sequence number instead of the random snapshot id, and an
   * {@code index} tag on each sub-batch so a pool node can dispatch by {@code (index, shard)}.
   */
  static BatchWriter writerFor(
      List<String> nodes,
      ShardRouter.Strategy routing,
      int[] bucketOwners,
      SnapshotLineage lineage,
      String index) {
    if (nodes.size() == 1) {
      // Carry the index tag on the single-endpoint path too: a one-shard index can still live on a
      // pool node serving many indexes, where an untagged write/checkpoint selector is ambiguous.
      String[] hp = nodes.get(0).split(":", 2);
      return new WriteClient(hp[0].trim(), Integer.parseInt(hp[1].trim()), index);
    }
    if (bucketOwners != null && bucketOwners.length > 0) {
      return new ShardedWriteClient(nodes, ShardRouter.bucketed(routing, bucketOwners), lineage, index);
    }
    return new ShardedWriteClient(nodes, new ShardRouter(nodes.size(), routing), lineage, index);
  }

  /**
   * Cap the JDK's positive DNS cache so a restarted Node's new pod IP is picked up within seconds.
   * {@code networkaddress.cache.ttl} is a <i>security</i> property, not a {@code -D}
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

  // --- S3 credentials: GrowlerDB namespace → Iceberg S3FileIO catalog props --------------------

  /**
   * GrowlerDB S3 creds → this catalog's Iceberg {@code S3FileIO} props (D55); a blank var is omitted
   * so S3FileIO uses the AWS default chain — IMDS/STS/IRSA (D56). {@code env} injected for testing.
   */
  static Map<String, String> s3CatalogConf(String catalog, Map<String, String> env) {
    String p = "spark.sql.catalog." + catalog + ".";
    Map<String, String> conf = new java.util.LinkedHashMap<>();
    putIfSet(conf, p + "s3.access-key-id", env.get("GROWLERDB_S3_ACCESS_KEY"));
    putIfSet(conf, p + "s3.secret-access-key", env.get("GROWLERDB_S3_SECRET_KEY"));
    putIfSet(conf, p + "client.region", env.get("GROWLERDB_S3_REGION"));
    return conf;
  }

  private static void putIfSet(Map<String, String> conf, String key, String value) {
    if (value != null && !value.isBlank()) {
      conf.put(key, value.trim());
    }
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
