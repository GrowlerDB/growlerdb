# Hydration axis — analyze the remap-heal hang, then step back and pick the right approach

Handoff for the agent taking the GrowlerDB-vs-OpenSearch benchmark's **full-document retrieval
(hydration) axis** to a clean landing. Read this whole file, then `bench/scale/comparison-plan.md`
(plan + fairness charter) and the memory note `os-comparison-bench.md`. Branch:
**`feat/os-comparison-bench`** (PR #322, draft).

## The bar (Kira, explicit)
- **Sub-second hydration is acceptable.** We are NOT chasing parity with OpenSearch's inline `_source`
  (~11 ms). GDB hydrating 20 documents from Iceberg in **< 1 s** is a fine, honest result — it's the
  disclosed `_source`-vs-hydrate storage tradeoff (charter #5: GDB stores 3.4× less, no `_source`).
- **Every other axis is already equal-or-better** and validated live: ingest (connector set ~2–4×
  OpenSearch), storage (GDB index < raw corpus, ~3.4× smaller than OS), freshness (~28× fresher),
  and index-only + autocomplete are competitive once hydration stops contaminating the mixed driver.
- **If the chosen approach does NOT rely on the fragile stored-locator code, the plan MUST clean up
  the residue** (the locator/#4/remap machinery added this round). See Task 4.

## What is already PROVEN on real clusters (don't re-derive)
5 paid re-shakedowns, all torn down clean, ~$25 total. On a 6× ccx43 nbg1 cluster, 10 GB
non-windowed CoW `http_logs`, ~25.9M rows:
- **Ingest:** the parallel connector SET (`CONNECTOR_SET=true`, 6 workers, one shard-group each)
  cold-syncs at ~30–77k docs/s vs OpenSearch Data Prepper ~17.5k. Convergence EXACT (dup-free).
- **PRE-compaction hydration is FAST:** `topk_hydrated` — the *real* benchmark query
  (`bench/scale/workloads/http_logs/queries.json`: `size:20`, `sort: response_time_ms desc`, so the 20
  result rows have `ts` scattered across the whole 7-day timeline) — ran **~0.9 s with all 20 hits**
  (over a ~0.5 s Mac→nbg1 curl RTT, so ~0.4 s in-cluster), down from a 30 s timeout, once:
  - the connector emits **real** `(file, position)` locators on the cold-sync backfill (**#4**), so
    hydration does **pass-1 point reads** instead of the placeholder-forced pass-2 scan; AND
  - parquet **row groups are 8 MiB** (`corpus.py` `write.parquet.row-group-size-bytes`) — a point read
    fetches the row group holding the row, so 20 rows ≈ 20×8 MB ≈ 160 MB. Verified live: compacted
    files are 197 MB with **9 MB row groups** (Spark honored the property).
- So the mechanism CAN be sub-second. The break is only **after compaction**.

## The ONE broken thing — the post-compaction bulk re-map heal HANGS
COORDINATES hydration stores `(file, position)` locators. An Iceberg compaction (`rewrite_data_files`,
a `replace` snapshot) rewrites every data file → every locator goes stale at once → they must be
**healed** (re-pointed to the new files) by the background poller `growlerdb_engine::remap_tick`
(`crates/growlerdb-engine/src/remap.rs`), or hydration falls to the slow pass-2 fallback.

**Live symptom (run 5, image with the "restart-safe" remap fix + #4 + 8 MB row groups):**
- The poller **detects** the rewrite and **marks files dead** — `growlerdb_locator_dead_files` climbs
  to ~6476 across the 6 shards and plateaus.
- But the **heal never completes**: `growlerdb_locator_remap_events_total` = **0**, no error logged,
  no `locator-remap` entry in `growlerdb_background_failures_total`, node CPU idle — **even after a
  node restart in a fully quiet window (queries killed, MinIO idle) for 5+ minutes.**
- Code flow says after `mark_files_dead` (remap.rs ~147) the tick must reach `Ok(Some)` (→ increments
  `locator_remap_events_total`) or `Err` (→ logs "poll failed" + `background_failure`). Neither
  happened. The only consistent explanation: the heal loop's `scan_added_file` →
  `read_file_key_rows` (reads the *compacted* files' key columns from MinIO via iceberg-rust/opendal)
  **hangs and never returns** — the tick never completes, so no metric/log fires.
- Note: the restart-safe fix (commit `41280fb`) re-derives heal candidates as **all plan files no
  shard already holds a live slot into** — which makes EACH of the 6 shards scan ALL current plan
  files (none interned-live yet) = ~6× the reads (~5–6 GB) on one MinIO. That may itself be part of
  the hang/slowness. An earlier local Polaris+MinIO repro (`crates/growlerdb-engine/tests/
  remap_tick_rest.rs`) showed the heal WORKING (100 slots) — so the repro did NOT capture this
  live failure (scale? the connector-written key encoding? the compacted-file read? MinIO?).

## Infra facts that matter
- MinIO is a **single pod on one node's local NVMe** (`deploy/k8s/deps/minio.yaml`, 20Gi PVC,
  unhardened) — a real object-store bottleneck under concurrency. Kira floated **Garage** as an
  alternative; hold it as a fallback, not the first move.
- Source table: unpartitioned, **hash-routed by a random `request_id`** (so a `request_id` predicate
  can't prune by file min/max), **ts-sorted** by compaction (`maintenance.yaml` runs `ALTER TABLE …
  WRITE ORDERED BY ts, request_id` — this sets `default-sort-order-id`, which the node reads).
- `request_id`/`trace_id`/`status` carry **parquet bloom filters** (`corpus.py` WRITE_PROPERTIES) —
  but note pyiceberg (ingest) does NOT write blooms; only the **Spark compaction** does.
- opendal S3 reads have a **10 s timeout + retries** — a persistently-failing read can retry for a
  long time (a plausible "hang").

---

# Task 1 — Analyze the remap-heal hang (understand it; you may not need to fix it)
Root-cause WHY `remap_tick`'s heal hangs on the compacted files, with evidence — ideally a faithful
local repro (extend `remap_tick_rest.rs` / the Polaris+MinIO `deploy/compose` stack) that reproduces
a HANG, not a clean heal. Candidates: (a) `scan_added_file`/`read_file_key_rows` reading a
compacted file (blooms? 29 row groups? projection?) hangs or is catastrophically slow via
iceberg-rust/opendal; (b) the restart-safe candidate set (every shard scans every plan file) makes it
~6× the work → minutes on one MinIO; (c) a key-encoding mismatch causing 0 patches (but that would
return `Ok(Some)`, not hang); (d) the poller's reader vs serving reader snapshot view. Add temporary
logging to `remap_tick` if a local repro won't reproduce it. **Do NOT burn paid clusters guessing —
this session already did that 5 times.**

# Task 2 — STEP BACK: is the stored-locator approach even right? (the important task)
Kira has flagged since early on that **the `(file,position)` locator is fragile — compaction wipes
locators en masse** — and would prefer an approach that doesn't depend on it. Given the sub-second
bar, seriously evaluate the alternative before investing more in the locator+remap path:

**The `PREDICATE` location strategy** (`location_strategy: PREDICATE` in the index def; already
implemented in the engine): store **NO** locators. Every hydration re-finds rows by a **key-equality
scan** pruned by partition/sort/bloom. Nothing to wipe, nothing to heal, no remap, no #4, no
staleness, no fragility. The whole failure mode above simply **cannot exist**.

The ONLY question: **can `PREDICATE` hydrate a 20-scattered-key top-k in sub-second?** That reduces to
whether Iceberg reads can skip to the **row groups** holding the wanted rows instead of scanning whole
files. Two independent skip mechanisms, both already set up:
- **`ts` (sort key):** each hit carries its `ts` (a fast field); the pass-2 predicate already ANDs
  `ts = <that row's ts>` (this session's `#3` sort-key prune-hint, in `key_predicate`). On a ts-sorted
  table, per-file AND **per-row-group** `ts` min/max are tight → a `ts` predicate can skip to the one
  row group per key.
- **`request_id` bloom:** the compacted files have a `request_id` bloom → a `request_id` predicate can
  skip row groups that can't contain the key.

**THE decisive unknown to resolve OFFLINE first (no cluster):** does **iceberg-rust 0.10.1** actually
perform **row-group-level** pruning on a scan read — via `src/expr/visitors/row_group_metrics_evaluator.rs`
(row-group stats) and/or parquet **bloom filters** — or only file-level pruning
(`inclusive_metrics_evaluator.rs`, which this session confirmed it does)? Read the iceberg-rust source
(`~/.cargo/registry/src/*/iceberg-0.10.1/src/arrow/` reader + `scan/mod.rs`) and/or write a local test:
a ts-sorted, 8 MB-row-group, request_id-bloomed table; a pass-2 scan with `request_id=X AND ts=T`;
measure **bytes/row-groups actually read** (not just files planned). If it reads ~1 row group (~8 MB)
per key → `PREDICATE` is sub-second and is the clean answer. If it reads whole files regardless →
`PREDICATE` can't hit sub-second on scattered keys and the locator/pass-1 path (Task 1 fix) is
required after all.

Deliver a clear recommendation: **`PREDICATE` (drop locators)** vs **`COORDINATES` (fix the remap
hang)**, grounded in that measurement. Favor `PREDICATE` if it meets the sub-second bar — it removes
the fragility Kira dislikes and the entire remap failure class.

# Task 3 — Implement the chosen approach and validate
- Make hydration sub-second for the real `topk_hydrated` query on a **freshly compacted** table
  (the failing case), proven by the in-cluster driver's per-type `service_ms` (RTT-free), not a
  Mac curl. Also confirm index-only + autocomplete come back clean (no more topk-timeout contamination
  of the mixed open-loop driver — consider measuring index-only/autocomplete in a SEPARATE driver pass
  regardless; the handoff's original #4 suggested this).
- Only ONE paid GDB-only re-shakedown after the fix is proven locally (generate → gdb_coldsync →
  growlerdb; skip OpenSearch, it's unchanged). Then, if clean, run the OpenSearch side for the final
  numbers and write `docs/benchmarks.md` + a RUNLOG row.

# Task 4 — If the plan drops the fragile locators: CLEAN UP THE RESIDUE
This session added a lot of machinery to prop up COORDINATES. If Task 2 lands on `PREDICATE`, remove
what's now dead/unvalidated so the PR is honest and minimal. Audit these (branch commits after
`ac42e16`):
- **REMOVE (locator-specific):** the connector real-locator backfill **(#4)** — `connector/.../
  ChangelogReader.java` + `ConnectorJob.java` backfill path + its tests (commit "revive #4"); the
  **remap changes made this round** — the streaming Fix B rewrite AND the "restart-safe" rewrite of
  `remap_tick` (commit `41280fb`) — the restart-safe one is BUGGY (hangs) and both are unvalidated;
  `remap_tick_rest.rs`; the `compare_run.py` remap **settle-gate** (`wait_remap_settled`, only meaningful
  if the remap runs). Decide whether to revert `remap.rs` fully to its pre-session state or keep a
  minimal honest version — but do NOT ship the hanging restart-safe version.
- **KEEP (approach-agnostic, genuine improvements):** 8 MiB row groups (`corpus.py`); `WRITE ORDERED BY`
  in `maintenance.yaml` (declares the ts sort — needed for ts pruning); the **`#3` sort-key
  prune-hint** in `key_predicate` + `HydrateRequest` + `Shard::prune_values` + `sort_field_names`
  (this is what makes a PREDICATE/pass-2 scan prune — it's the core of the clean approach); the
  byte-budget in `scan_stale_index` (safety net); the robust-key-predicate skip (commit `7ec3db3`);
  the request_id/ts/status blooms; T1 connector-set; T2 completion sidecar; T5 raw-basis; T6 replicas=0.
- **SWITCH:** `bench/scale/workloads/http_logs/index.yaml` → `location_strategy: PREDICATE` (verify the
  exact key/field-mapping syntax in the index-def schema; today it's the default COORDINATES).
- Re-run `just okf-check` and update the OKF (`okf/product/functional/hydration.md`,
  `okf/system/decisions/d30-layered-locator.md`) to reflect the chosen strategy; drop the OKF notes
  that describe the removed machinery. Keep the OKF honest about what shipped.

---

# Validate + run mechanics
- Local gate before ANY paid run: `just check`-relevant pieces — `cargo build/test/clippy/fmt` for
  touched crates, `okf-check`, the python self-checks. **Prove the fix locally first** (this session's
  repeated mistake was validating on paid clusters).
- Engine changes → `gh workflow run scale-images.yml --ref feat/os-comparison-bench` (no `-f tag` →
  tags `:dev` + the commit SHA; builds engine + connector + seed matrix). Use the **SHA** as
  `IMAGE_TAG` for the engine; the connector-set template pulls `:dev` (`imagePullPolicy: Always`).
- Run recipe (GDB-only, ~$3–5, ~45 min): `terraform apply` (deploy/iac; tfvars gitignored, 6× ccx43
  nbg1; admin_ssh_cidr must be your current IP) → kubeconfig via SSH (`~/.ssh/id_ed25519_h`) →
  `STAGE=deps … deploy/k8s/scale-up.sh` (deps+generator+observability) → detached prometheus+gateway
  port-forwards → `compare_run.py --phase generate|gdb_coldsync|growlerdb`. Creds: `~/.ssh/gdb-license`
  (GROWLERDB_LICENSE), GHCR PAT from `~/.docker/config.json`, `~/.ssh/hcloud_token`. **Always
  `terraform destroy` at the end and verify `hcloud server list` is empty.**

# Guardrails (hard-won this round — do not repeat)
- **Never declare a hydration fix working from a Mac→nbg1 curl** — it carries ~0.5 s RTT; use the
  in-cluster driver's `service_ms` or engine-internal Prometheus histograms. And a "fast" repeat query
  may just be a back-patched locator, not the mechanism under test — measure on a FRESH compacted state.
- **Bash-tool `run_in_background` processes get reaped** mid-run; launch long runs with `nohup … &
  disown` (survives). `setsid` isn't on macOS. Foreground `sleep` is blocked — use `until <cond>; do
  sleep N; done`.
- **Prove it locally before spending.** Two local repros this session (StaticTable prune; Polaris-REST
  prune) each "passed" but missed the live failure because they weren't faithful (wrong sort-order
  mechanism; heal worked in-repro but hangs live). Make the repro reproduce the ACTUAL failure before
  trusting it.
- Keep intact everything the memory note lists as validated (dup fix `d7fc862`, restart-safety
  `79758d6`, Spark-4.0.0 maintenance, the bounded-drain query-driver guard, T1 connector-set).
