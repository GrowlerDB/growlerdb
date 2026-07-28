//! D53 chaos drill (357.10, honest edition 357.23): **zero-gap read failover under sustained
//! query**. Two parked windows live in a shared local-filesystem object store
//! (`GROWLERDB_OBJECT_STORE_FS` — no MinIO needed). A control plane at `R=2` places two pool nodes
//! as each window's holders (primary + replica); both open them **read-through** from the object
//! store via the CP assignment push. A gateway built **before** the kill (so established channels
//! are exercised, not a fresh dial) scatters every read across both windows through a
//! `FailoverNode` per window — and when the **primary node is killed under sustained query**, reads
//! keep answering via the replica with a **bounded gap and no `partial`** (two windows put every
//! query on the scatter path, where `partial` is structurally reachable — so asserting its absence
//! means something).

use std::collections::BTreeMap;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use growlerdb_core::{
    CommitBatch, CompositeKey, Document, IndexDefinition, IndexWriter, LocatedDoc, ResolvedIndex,
    SourceCheckpoint, SourceField, SourceSchema, SourceType, TimeWindowing, Value,
    WindowGranularity,
};
use growlerdb_engine::{FailoverNode, Gateway, Node, RemoteNode, ShardNode, WindowNode};
use growlerdb_index::{LocalIndexStore, ShardId};
use growlerdb_proto::v1::control_plane_client::ControlPlaneClient;
use growlerdb_proto::v1::write_client::WriteClient;
use growlerdb_proto::v1::{
    resolve_unit_owner_request, GetCheckpointRequest, ListIndexesRequest, ResolveUnitOwnerRequest,
    SearchRequest, WriteRequest,
};
use tonic::transport::Channel;
use tonic::Request;

const IDX: &str = "logs";
/// Two windows so every drill query takes the multi-shard scatter path (single-window reads
/// short-circuit before `partial` can ever be set — a no-`partial` assertion there is vacuous).
const W1: i64 = 10;
const W2: i64 = 11;
const DOC1: &str = "doc-1";
const DOC2: &str = "doc-2";

fn windowed_index() -> ResolvedIndex {
    let src = SourceSchema::new(
        vec![
            SourceField::new("id", SourceType::String),
            SourceField::new("ts", SourceType::Long),
        ],
        vec![],
        vec!["id".into()],
    );
    IndexDefinition::from_yaml(
        "name: logs\nsource: { iceberg: { catalog: g, table: g.logs } }\nwindowing: { field: ts, granularity: daily }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD, fast: true }, { path: ts, format: epoch_ms, fast: true } ] }\n",
    )
    .unwrap()
    .resolve(&src)
    .unwrap()
}

/// Write `{data_dir}/logs/index.json` (the definition only, no window shards) — a node started here
/// serves the windows read-through, not from local data.
fn define_index(data_dir: &std::path::Path) {
    std::fs::create_dir_all(data_dir.join(IDX)).unwrap();
    std::fs::write(
        data_dir.join(IDX).join("index.json"),
        serde_json::to_vec(&windowed_index()).unwrap(),
    )
    .unwrap();
}

/// Park `window` (holding one doc `id`) into the shared object store, from a throwaway build dir.
async fn park_window(store_dir: &std::path::Path, window: i64, id: &str) {
    let build = tempfile::tempdir().unwrap();
    let store = LocalIndexStore::open(build.path()).unwrap();
    let resolved = windowed_index();
    let shard_id = ShardId::window(IDX, window);
    let shard = store.create_shard(&shard_id, &resolved).unwrap();
    let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]);
    let mut f = BTreeMap::new();
    f.insert("id".to_string(), Value::from(id));
    f.insert("ts".to_string(), Value::from(1_i64));
    IndexWriter::write(
        &shard,
        &CommitBatch::from_upserts(
            vec![LocatedDoc {
                doc: Document::new(key, f),
                iceberg_file: "f".into(),
                row_position: 0,
            }],
            SourceCheckpoint::iceberg(1),
            "b1",
        ),
    )
    .unwrap();
    let op = growlerdb_backup::fs_store(store_dir).unwrap();
    growlerdb_backup::cold_park(
        shard,
        IDX,
        window,
        &store.shard_path(&shard_id),
        &build.path().join(".stg"),
        &op,
        &format!("cold/{IDX}/w{window}"),
        Some(serde_json::to_string(&resolved).unwrap()),
    )
    .await
    .unwrap();
}

/// A spawned `growlerdb` process, killed on drop. `kill()` stops it early (to simulate a node loss).
struct Proc(Child);
impl Proc {
    fn kill(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
impl Drop for Proc {
    fn drop(&mut self) {
        self.kill();
    }
}

/// Reserve `n` **distinct** free ports by binding all `n` listeners at once (so the OS hands out
/// different ports), then dropping them. Binding one at a time can hand back the same port twice —
/// the listener is released before the next call — and two processes would then collide on it.
fn free_addrs(n: usize) -> Vec<String> {
    let listeners: Vec<_> = (0..n)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    listeners
        .iter()
        .map(|l| format!("127.0.0.1:{}", l.local_addr().unwrap().port()))
        .collect()
}

async fn wait_for_grpc(endpoint: &str) {
    for _ in 0..200 {
        if RemoteNode::connect(endpoint.to_string(), None)
            .await
            .is_ok()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("process did not come up at {endpoint}");
}

/// Serialize the port-reservation → process-bind window across this file's concurrent tests.
/// [`free_addrs`] reserves ports by binding `:0` and releasing them; a sibling test that spawns in
/// that release window can grab the same port, and this test would then connect to a *sibling's*
/// control plane (seen as `resolve` returning a foreign node's endpoint). `cargo test` runs test
/// binaries serially, so the only contention is among this file's own parallel tests — each holds
/// this guard for its duration.
static STARTUP: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// A gRPC-level control-plane readiness gate. A bare TCP/HTTP2 connect can succeed before the CP is
/// answering, so nodes spawned right after would spin their `--register` on transport errors and,
/// under load, miss the CP's post-boot liveness grace (leaving a unit's holder set frozen at one
/// node). Gate the CP with this BEFORE spawning nodes: only a gRPC reply — `Ok`, or an application
/// `Status` with no transport source — proves the CP is actually serving.
async fn wait_for_cp_ready(cp_ep: &str) {
    for _ in 0..600 {
        if let Ok(mut c) = ControlPlaneClient::connect(cp_ep.to_string()).await {
            match c
                .list_indexes(Request::new(ListIndexesRequest::default()))
                .await
            {
                Ok(_) => return,
                Err(status) if std::error::Error::source(&status).is_none() => return,
                Err(_) => {}
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("control plane did not become ready at {cp_ep}");
}

/// Poll a node's `/readyz` (its `--metrics-addr`) until it reports ready — i.e. the node's first
/// pool registration succeeded. The R=2 drills must hold the FIRST resolve until **both** nodes
/// are in the placement pool: a resolve that lands while only one node has registered places a
/// single holder, and the CP's post-boot liveness grace (one heartbeat TTL) then freezes the
/// assigned unit's holder set — the late node would never be topped up within the test budget.
async fn wait_until_ready(metrics_addr: &str) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    for _ in 0..600 {
        if let Ok(mut s) = tokio::net::TcpStream::connect(metrics_addr).await {
            let req = format!("GET /readyz HTTP/1.0\r\nHost: {metrics_addr}\r\n\r\n");
            if s.write_all(req.as_bytes()).await.is_ok() {
                let mut buf = Vec::new();
                let _ = s.read_to_end(&mut buf).await;
                let head = String::from_utf8_lossy(&buf);
                if head.lines().next().is_some_and(|l| l.contains(" 200")) {
                    return;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("node at {metrics_addr} never became ready (pool registration)");
}

/// The sorted `id`s a windowed gateway returns for a `*` search (window unset → the gateway fans to
/// every window). A transport error *or* an honest `partial` is an `Err` — for this drill a
/// degraded page with a live replica available is exactly as much a failure as no page at all.
async fn search_ids(gw: &Gateway) -> Result<Vec<String>, String> {
    search_ids_idx(gw, IDX).await
}

/// As [`search_ids`] but for `index` — the hash drill fronts a different index than the windowed one.
async fn search_ids_idx(gw: &Gateway, index: &str) -> Result<Vec<String>, String> {
    let resp = gw
        .search(Request::new(SearchRequest {
            query: "*".into(),
            limit: 10,
            index: index.into(),
            ..Default::default()
        }))
        .await
        .map_err(|s| format!("{:?}: {}", s.code(), s.message()))?
        .into_inner();
    if resp.partial {
        return Err("partial response (a unit degraded instead of failing over)".into());
    }
    let mut ids: Vec<String> = resp
        .hits
        .iter()
        .filter_map(|h| {
            h.coordinates
                .as_ref()
                .and_then(|c| c.identifier.iter().find(|f| f.name == "id"))
                .and_then(|f| f.value.clone())
                .and_then(|v| match v.kind {
                    Some(growlerdb_proto::v1::value::Kind::Str(s)) => Some(s),
                    _ => None,
                })
        })
        .collect();
    ids.sort();
    Ok(ids)
}

/// A windowed gateway over `units` (`(window, primary endpoint, replica endpoint)`), each behind a
/// `FailoverNode` over `[primary, replica]` — the D53 read path the cluster gateway builds from the
/// CP's holder set. Lazy connect (as the real gateway does): building a holder over a DOWN endpoint
/// must not fail — it fails fast at query time, which is exactly what lets the FailoverNode fall
/// over to the replica once the primary is killed.
fn failover_gateway(units: &[(i64, String, String)]) -> Gateway {
    let holder = |ep: &str, w: i64| {
        let remote = RemoteNode::connect_lazy(ep.to_string(), None).unwrap();
        WindowNode::new(Arc::new(remote), IDX, w).shared() as Arc<dyn Node>
    };
    let mut nodes: Vec<Arc<dyn Node>> = Vec::with_capacity(units.len());
    let mut descriptors = Vec::with_capacity(units.len());
    for (w, primary_ep, replica_ep) in units {
        nodes
            .push(FailoverNode::new(holder(primary_ep, *w), vec![holder(replica_ep, *w)]).shared());
        descriptors.push((*w, None, true));
    }
    Gateway::windowed(
        nodes,
        TimeWindowing::new("ts", WindowGranularity::Daily),
        descriptors,
    )
}

/// Poll `endpoint` (a lone `WindowNode` over `(IDX, window)`) until it serves the parked window's
/// doc — i.e. the node has picked up the CP assignment and opened it read-through. The budget is
/// generous (30 s) because the tests in this file run in parallel: several CP + node processes
/// each cold-opening windows contend for CPU/IO, and the pre-serve convergence (register →
/// subscribe → push → cold open) is exactly what slows down under that load.
async fn wait_until_serving(endpoint: &str, window: i64, doc: &str) {
    for _ in 0..600 {
        if let Ok(remote) = RemoteNode::connect(endpoint.to_string(), None).await {
            let node: Arc<dyn Node> = WindowNode::new(Arc::new(remote), IDX, window).shared();
            let gw = Gateway::windowed(
                vec![node],
                TimeWindowing::new("ts", WindowGranularity::Daily),
                vec![(window, None, true)],
            );
            if let Ok(ids) = search_ids(&gw).await {
                if ids == vec![doc.to_string()] {
                    return;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("{endpoint} never served window {window} read-through");
}

/// Resolve `window`'s primary endpoint from the CP, retrying until the index is registered (the
/// nodes have announced) and a live holder is placed.
async fn resolve_primary(cp: &mut ControlPlaneClient<Channel>, window: i64) -> String {
    loop {
        match cp
            .resolve_unit_owner(Request::new(ResolveUnitOwnerRequest {
                index: IDX.into(),
                unit: Some(resolve_unit_owner_request::Unit::Window(window)),
            }))
            .await
        {
            Ok(r) => return r.into_inner().endpoint,
            // Index not registered yet / no live node yet — retry.
            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
}

#[tokio::test]
async fn killing_a_units_primary_fails_reads_over_to_the_replica() {
    // Serialize the port-reservation/process-bind window against this file's other tests.
    let _startup = STARTUP.lock().await;
    // A shared local-fs object store both nodes read the parked windows through — no S3/MinIO.
    let store_dir = tempfile::tempdir().unwrap();

    // 1. Park TWO windows (one doc each) into the shared object store — every drill query then
    //    scatters across two shards, the path where `partial` is reachable.
    park_window(store_dir.path(), W1, DOC1).await;
    park_window(store_dir.path(), W2, DOC2).await;

    // 2. Two node data dirs with the definition only (they serve the windows read-through).
    let a_dir = tempfile::tempdir().unwrap();
    let b_dir = tempfile::tempdir().unwrap();
    define_index(a_dir.path());
    define_index(b_dir.path());

    // Distinct ports for the CP + the two nodes + their health endpoints (bound together so they
    // can't collide).
    let addrs = free_addrs(5);
    let (cp_addr, a_addr, b_addr) = (addrs[0].clone(), addrs[1].clone(), addrs[2].clone());
    let (a_health, b_health) = (addrs[3].clone(), addrs[4].clone());

    // 3. Control plane at R=2.
    let cp_dir = tempfile::tempdir().unwrap();
    let _cp = Proc(
        Command::new(env!("CARGO_BIN_EXE_growlerdb"))
            .args([
                "--data-dir",
                cp_dir.path().to_str().unwrap(),
                "control-plane",
                "--addr",
                &cp_addr,
            ])
            .env("GROWLERDB_REPLICATION_FACTOR", "2")
            .stdout(Stdio::null())
            .spawn()
            .expect("spawn control-plane"),
    );
    let cp_ep = format!("http://{cp_addr}");
    // The CP must be serving before the nodes spawn, so their `--register` lands cleanly.
    wait_for_cp_ready(&cp_ep).await;

    // 4. Two pool nodes: registered into the pool, reading the parked windows through the shared store.
    let a_ep = format!("http://{a_addr}");
    let b_ep = format!("http://{b_addr}");
    let spawn_node = |dir: &std::path::Path, addr: &str, ep: &str, health: &str| {
        Proc(
            Command::new(env!("CARGO_BIN_EXE_growlerdb"))
                .args([
                    "--data-dir",
                    dir.to_str().unwrap(),
                    "--metrics-addr",
                    health,
                    "serve-pool",
                    "--index",
                    IDX,
                    "--addr",
                    addr,
                    "--register",
                    &cp_ep,
                    "--advertise-addr",
                    ep,
                ])
                .env("GROWLERDB_OBJECT_STORE_FS", store_dir.path())
                .stdout(Stdio::null())
                .spawn()
                .expect("spawn serve-pool"),
        )
    };
    let mut node_a = spawn_node(a_dir.path(), &a_addr, &a_ep, &a_health);
    let mut node_b = spawn_node(b_dir.path(), &b_addr, &b_ep, &b_health);
    wait_for_grpc(&a_ep).await;
    wait_for_grpc(&b_ep).await;
    // BOTH nodes must be pool-registered before the first resolve — see `wait_until_ready`.
    wait_until_ready(&a_health).await;
    wait_until_ready(&b_health).await;

    // 5. Place both windows (R=2 → primary + replica each; with two nodes, both hold both windows).
    let mut cp = {
        let mut client = None;
        for _ in 0..200 {
            if let Ok(c) = ControlPlaneClient::connect(cp_ep.clone()).await {
                client = Some(c);
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        client.expect("connect control plane")
    };
    let other = |primary: &str| {
        if primary == a_ep {
            b_ep.clone()
        } else {
            a_ep.clone()
        }
    };
    let w1_primary = resolve_primary(&mut cp, W1).await;
    let w2_primary = resolve_primary(&mut cp, W2).await;
    assert!(w1_primary == a_ep || w1_primary == b_ep);
    let units = vec![
        (W1, w1_primary.clone(), other(&w1_primary)),
        (W2, w2_primary.clone(), other(&w2_primary)),
    ];

    // 6. Every holder serves both windows read-through once the CP push reaches them.
    for ep in [&a_ep, &b_ep] {
        wait_until_serving(ep, W1, DOC1).await;
        wait_until_serving(ep, W2, DOC2).await;
    }

    // 7. The drill gateway is built BEFORE the kill and used across it — established channels to
    //    the doomed primary are exactly the mode a fresh post-kill gateway would sidestep. Sustain
    //    queries against it pre-kill: both windows answer, never partial.
    let gw = failover_gateway(&units);
    let both = vec![DOC1.to_string(), DOC2.to_string()];
    for _ in 0..10 {
        assert_eq!(
            search_ids(&gw).await.expect("pre-kill query"),
            both,
            "both windows answer before the kill"
        );
    }

    // 8. Kill window W1's PRIMARY node under the ongoing query stream.
    let killed_at = Instant::now();
    if w1_primary == a_ep {
        node_a.kill();
    } else {
        node_b.kill();
    }

    // 9. Sustained queries across the kill on the SAME gateway. The failover gap must be bounded:
    //    within a short grace (the OS tearing down the dead process's sockets) reads may error, but
    //    after it EVERY query answers both docs with no partial — the replica absorbed the loss and
    //    the gap never reopens. 40 consecutive post-kill successes prove sustained recovery.
    const GRACE: Duration = Duration::from_millis(1000);
    let mut successes = 0u32;
    let mut gap_failures = 0u32;
    for _ in 0..400 {
        match search_ids(&gw).await {
            Ok(ids) => {
                assert_eq!(ids, both, "a failover answer must still cover both windows");
                successes += 1;
                if successes >= 40 {
                    break;
                }
            }
            Err(e) => {
                assert!(
                    killed_at.elapsed() <= GRACE,
                    "read failed {:?} after the kill (past the {GRACE:?} failover grace, \
                     {successes} successes so far): {e}",
                    killed_at.elapsed()
                );
                successes = 0; // the gap is only closed once successes are consecutive
                gap_failures += 1;
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        successes >= 40,
        "no sustained recovery after the primary kill ({gap_failures} failures in the gap)"
    );

    // 10. A FRESH gateway (new channels dialing the dead primary) also answers — the cold-dial
    //     failover mode, kept from the original drill.
    assert_eq!(
        search_ids(&failover_gateway(&units))
            .await
            .expect("fresh-gateway query after the kill"),
        both,
        "a fresh gateway fails over past the dead primary too"
    );
}

/// HA-G1 end-to-end: a unit the CP **de-assigns is unloaded** — the node stops serving it (the
/// structured `UNIT_NOT_SERVED` refusal, as if it never held it) and the unit's `.replica`
/// read-through scratch is deleted after the short drain grace. `DropIndex` is the cleanest
/// de-assignment driver: the CP's next pushed snapshot simply no longer contains the unit.
#[tokio::test]
async fn a_deassigned_unit_is_unloaded_and_its_replica_scratch_cleaned() {
    // Serialize the port-reservation/process-bind window against this file's other tests.
    let _startup = STARTUP.lock().await;
    use growlerdb_proto::v1::search_client::SearchClient;
    use growlerdb_proto::v1::DropIndexRequest;

    let store_dir = tempfile::tempdir().unwrap();
    park_window(store_dir.path(), W1, DOC1).await;

    let a_dir = tempfile::tempdir().unwrap();
    define_index(a_dir.path());

    let addrs = free_addrs(2);
    let (cp_addr, a_addr) = (addrs[0].clone(), addrs[1].clone());
    let cp_dir = tempfile::tempdir().unwrap();
    let _cp = Proc(
        Command::new(env!("CARGO_BIN_EXE_growlerdb"))
            .args([
                "--data-dir",
                cp_dir.path().to_str().unwrap(),
                "control-plane",
                "--addr",
                &cp_addr,
            ])
            .stdout(Stdio::null())
            .spawn()
            .expect("spawn control-plane"),
    );
    let cp_ep = format!("http://{cp_addr}");
    // The CP must be serving before the nodes spawn, so their `--register` lands cleanly.
    wait_for_cp_ready(&cp_ep).await;
    let a_ep = format!("http://{a_addr}");
    let _node = Proc(
        Command::new(env!("CARGO_BIN_EXE_growlerdb"))
            .args([
                "--data-dir",
                a_dir.path().to_str().unwrap(),
                "serve-pool",
                "--index",
                IDX,
                "--addr",
                &a_addr,
                "--register",
                &cp_ep,
                "--advertise-addr",
                &a_ep,
            ])
            .env("GROWLERDB_OBJECT_STORE_FS", store_dir.path())
            .stdout(Stdio::null())
            .spawn()
            .expect("spawn serve-pool"),
    );
    wait_for_grpc(&a_ep).await;

    let mut cp = {
        let mut client = None;
        for _ in 0..200 {
            if let Ok(c) = ControlPlaneClient::connect(cp_ep.clone()).await {
                client = Some(c);
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        client.expect("connect control plane")
    };
    let primary = resolve_primary(&mut cp, W1).await;
    assert_eq!(primary, a_ep, "the sole node holds the unit");
    wait_until_serving(&a_ep, W1, DOC1).await;
    let scratch = a_dir
        .path()
        .join(".replica")
        .join(IDX)
        .join(format!("w{W1}"));
    assert!(
        scratch.is_dir(),
        "read-through scratch exists while the unit is assigned"
    );

    // De-assign: drop the index — the CP pushes a snapshot without the unit.
    cp.drop_index(Request::new(DropIndexRequest { name: IDX.into() }))
        .await
        .expect("drop index");

    // The node UNLOADS the window: the very shard it just answered from becomes the structured
    // not-served refusal (pre-fix it kept serving the de-assigned unit forever).
    let mut search = SearchClient::connect(a_ep.clone()).await.unwrap();
    let mut unloaded = false;
    for _ in 0..400 {
        let res = search
            .search(SearchRequest {
                query: "*".into(),
                limit: 10,
                index: IDX.into(),
                window: W1,
                ..Default::default()
            })
            .await;
        if let Err(status) = res {
            assert_eq!(status.code(), tonic::Code::FailedPrecondition);
            unloaded = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(unloaded, "the de-assigned unit was never unloaded");

    // …and its scratch is deleted once the drain grace elapses (pre-fix it accumulated forever).
    let mut cleaned = false;
    for _ in 0..400 {
        if !scratch.exists() {
            cleaned = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        cleaned,
        "the de-assigned unit's replica scratch was never cleaned"
    );
}

/// 357.12 (HA-A1..A3) end-to-end: the node-side **write fence**. With a CP at R=2 both nodes hold a
/// parked window — one as primary, one as replica. A `Write` / `GetCheckpoint` addressed to the
/// REPLICA holder is refused with the structured `NOT_PRIMARY` detail (never committed, never a
/// fabricated empty checkpoint); a `Write` to the PRIMARY holder of the parked window is refused
/// `WINDOW_PARKED` instead of overwriting the served snapshot with a fresh empty shard (HA-A2). And
/// after both refusals, both holders still serve the parked doc read-through — nothing was clobbered.
#[tokio::test]
async fn a_write_to_a_replica_held_window_is_fenced_and_the_parked_data_survives() {
    // Serialize the port-reservation/process-bind window against this file's other tests.
    let _startup = STARTUP.lock().await;
    // A window id on a real daily boundary, so a written doc's `ts` routes to exactly this window.
    const DAY_US: i64 = 86_400_000_000;
    const DAY_MS: i64 = 86_400_000;
    const WD: i64 = 10 * DAY_US;

    let store_dir = tempfile::tempdir().unwrap();
    park_window(store_dir.path(), WD, DOC1).await;

    let a_dir = tempfile::tempdir().unwrap();
    let b_dir = tempfile::tempdir().unwrap();
    define_index(a_dir.path());
    define_index(b_dir.path());

    let addrs = free_addrs(5);
    let (cp_addr, a_addr, b_addr) = (addrs[0].clone(), addrs[1].clone(), addrs[2].clone());
    let (a_health, b_health) = (addrs[3].clone(), addrs[4].clone());

    let cp_dir = tempfile::tempdir().unwrap();
    let _cp = Proc(
        Command::new(env!("CARGO_BIN_EXE_growlerdb"))
            .args([
                "--data-dir",
                cp_dir.path().to_str().unwrap(),
                "control-plane",
                "--addr",
                &cp_addr,
            ])
            .env("GROWLERDB_REPLICATION_FACTOR", "2")
            .stdout(Stdio::null())
            .spawn()
            .expect("spawn control-plane"),
    );
    let cp_ep = format!("http://{cp_addr}");
    // The CP must be serving before the nodes spawn, so their `--register` lands cleanly.
    wait_for_cp_ready(&cp_ep).await;

    let a_ep = format!("http://{a_addr}");
    let b_ep = format!("http://{b_addr}");
    let spawn_node = |dir: &std::path::Path, addr: &str, ep: &str, health: &str| {
        Proc(
            Command::new(env!("CARGO_BIN_EXE_growlerdb"))
                .args([
                    "--data-dir",
                    dir.to_str().unwrap(),
                    "--metrics-addr",
                    health,
                    "serve-pool",
                    "--index",
                    IDX,
                    "--addr",
                    addr,
                    "--register",
                    &cp_ep,
                    "--advertise-addr",
                    ep,
                ])
                .env("GROWLERDB_OBJECT_STORE_FS", store_dir.path())
                .stdout(Stdio::null())
                .spawn()
                .expect("spawn serve-pool"),
        )
    };
    let _node_a = spawn_node(a_dir.path(), &a_addr, &a_ep, &a_health);
    let _node_b = spawn_node(b_dir.path(), &b_addr, &b_ep, &b_health);
    wait_for_grpc(&a_ep).await;
    wait_for_grpc(&b_ep).await;
    // BOTH nodes must be pool-registered before the first resolve — see `wait_until_ready`.
    wait_until_ready(&a_health).await;
    wait_until_ready(&b_health).await;

    // Place the window (R=2 → with two nodes, one primary + one replica) and wait until BOTH
    // holders serve it read-through — which also proves each processed an assignment snapshot, so
    // the write fence on each is armed with its role.
    let mut cp = {
        let mut client = None;
        for _ in 0..200 {
            if let Ok(c) = ControlPlaneClient::connect(cp_ep.clone()).await {
                client = Some(c);
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        client.expect("connect control plane")
    };
    let primary_ep = resolve_primary(&mut cp, WD).await;
    assert!(primary_ep == a_ep || primary_ep == b_ep);
    let replica_ep = if primary_ep == a_ep {
        b_ep.clone()
    } else {
        a_ep.clone()
    };
    wait_until_serving(&primary_ep, WD, DOC1).await;
    wait_until_serving(&replica_ep, WD, DOC1).await;

    // A one-doc batch whose `ts` (epoch-ms) buckets into WD.
    let batch = || {
        let key = CompositeKey::new(vec![], vec![("id".into(), Value::from("intruder"))]);
        let mut f = BTreeMap::new();
        f.insert("id".to_string(), Value::from("intruder"));
        f.insert("ts".to_string(), Value::from(10 * DAY_MS));
        let b: growlerdb_proto::v1::DocBatch = CommitBatch::from_upserts(
            vec![LocatedDoc {
                doc: Document::new(key, f),
                iceberg_file: "f".into(),
                row_position: 0,
            }],
            SourceCheckpoint::iceberg(9),
            "intrude-1",
        )
        .into();
        b
    };

    // HA-A1/A2: the REPLICA holder refuses the write with the structured NOT_PRIMARY detail —
    // previously it silently created a fresh empty shard and overwrote the served parked snapshot.
    let mut writer = WriteClient::connect(replica_ep.clone()).await.unwrap();
    let err = writer
        .write(WriteRequest {
            batch: Some(batch()),
            index: IDX.into(),
            shard: 0,
        })
        .await
        .expect_err("a write to a replica-held window must be refused");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(
        growlerdb_engine::is_not_primary(&err),
        "the structured NOT_PRIMARY detail survives the wire: {err:?}"
    );

    // HA-A3: GetCheckpoint on the replica holder is the same refusal — never "no checkpoint,
    // snapshot 0", which would instruct a connector to re-ingest the window here from scratch.
    let err = writer
        .get_checkpoint(GetCheckpointRequest {
            window: WD,
            index: IDX.into(),
            shard: 0,
        })
        .await
        .expect_err("a checkpoint read on a replica-held window must be refused");
    assert!(
        growlerdb_engine::is_not_primary(&err),
        "checkpoint refusal carries NOT_PRIMARY: {err:?}"
    );

    // HA-A2 (primary side): the PRIMARY holds this window as a parked read-through snapshot — a
    // write is WINDOW_PARKED (revive/reroute), not an overwrite with a fresh empty shard.
    let mut writer = WriteClient::connect(primary_ep.clone()).await.unwrap();
    let err = writer
        .write(WriteRequest {
            batch: Some(batch()),
            index: IDX.into(),
            shard: 0,
        })
        .await
        .expect_err("a write into the primary's parked window must be refused");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        growlerdb_proto::error_details(&err)
            .expect("structured error")
            .code,
        "WINDOW_PARKED"
    );

    // After all refusals, BOTH holders still serve the parked doc — the mux entries are intact.
    wait_until_serving(&primary_ep, WD, DOC1).await;
    wait_until_serving(&replica_ep, WD, DOC1).await;
}

// ── Hash-shard failover (D53 parity) ──────────────────────────────────────────────────────────────
// The windowed drill above proves zero-gap read failover for a windowed index. This one proves the
// same for a **hash-sharded** index: a TWO-ordinal index whose shards live frozen in the shared
// object store (published by `backup_replica_snapshot`), placed at R=2 so both pool nodes open each
// ordinal read-through. Two ordinals put every `*` search on the scatter path (where `partial` is
// reachable), so asserting its absence across a primary kill means something — the hash counterpart
// of the two-window setup.

const HASH_IDX: &str = "docs";
const ORD0_DOC: &str = "ord0doc";
const ORD1_DOC: &str = "ord1doc";

/// A minimal **hash** index (`id` KEYWORD, no windowing) sharded into two ordinals.
fn hash_index() -> ResolvedIndex {
    let src = SourceSchema::new(
        vec![SourceField::new("id", SourceType::String)],
        vec![],
        vec!["id".into()],
    );
    IndexDefinition::from_yaml(
        "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nshard_count: 2\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD, fast: true } ] }\n",
    )
    .unwrap()
    .resolve(&src)
    .unwrap()
}

/// Write `{data_dir}/docs/index.json` (the definition only, no ordinal shards) — a node started here
/// serves the ordinals read-through from the object store, not from local data.
fn define_hash_index(data_dir: &std::path::Path) {
    std::fs::create_dir_all(data_dir.join(HASH_IDX)).unwrap();
    std::fs::write(
        data_dir.join(HASH_IDX).join("index.json"),
        serde_json::to_vec(&hash_index()).unwrap(),
    )
    .unwrap();
}

/// Publish ordinal `ordinal` (holding one doc `id`) to the shared object store as a frozen replica
/// snapshot — the hash counterpart to [`park_window`]. Built in a throwaway dir; the marker lands at
/// `cold/docs/{ordinal}` for a replica to open read-through.
async fn seed_ordinal(store_dir: &std::path::Path, ordinal: u32, id: &str) {
    let build = tempfile::tempdir().unwrap();
    let store = LocalIndexStore::open(build.path()).unwrap();
    let resolved = hash_index();
    let shard_id = ShardId::shard(HASH_IDX, ordinal);
    let shard = store.create_shard(&shard_id, &resolved).unwrap();
    let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]);
    let mut f = BTreeMap::new();
    f.insert("id".to_string(), Value::from(id));
    IndexWriter::write(
        &shard,
        &CommitBatch::from_upserts(
            vec![LocatedDoc {
                doc: Document::new(key, f),
                iceberg_file: "f".into(),
                row_position: 0,
            }],
            SourceCheckpoint::iceberg(1),
            "b1",
        ),
    )
    .unwrap();
    let op = growlerdb_backup::fs_store(store_dir).unwrap();
    growlerdb_backup::backup_replica_snapshot(
        &shard,
        HASH_IDX,
        &ordinal.to_string(),
        &build.path().join(".stg"),
        &op,
        &format!("cold/{HASH_IDX}/{ordinal}"),
        Some(serde_json::to_string(&resolved).unwrap()),
    )
    .await
    .unwrap();
}

/// A **sharded** gateway over `units` (`(ordinal, primary endpoint, replica endpoint)`, in ordinal
/// order), each behind a `FailoverNode` over `[ShardNode(primary), ShardNode(replica)]` — the D53 hash
/// read path the cluster gateway builds from the CP's holder set. Lazy connect, as the real gateway.
fn sharded_failover_gateway(units: &[(u32, String, String)]) -> Gateway {
    let holder = |ep: &str, o: u32| {
        let remote = RemoteNode::connect_lazy(ep.to_string(), None).unwrap();
        ShardNode::new(Arc::new(remote), HASH_IDX, o).shared() as Arc<dyn Node>
    };
    let mut nodes: Vec<Arc<dyn Node>> = Vec::with_capacity(units.len());
    for (o, primary_ep, replica_ep) in units {
        nodes
            .push(FailoverNode::new(holder(primary_ep, *o), vec![holder(replica_ep, *o)]).shared());
    }
    Gateway::sharded(nodes)
}

/// Poll `endpoint` (a lone `ShardNode` over `(HASH_IDX, ordinal)`) until it serves the ordinal's doc —
/// i.e. the node picked up the CP assignment and opened it read-through. Generous budget: parallel
/// tests contend for CPU/IO during the register → subscribe → push → cold-open convergence.
async fn wait_until_serving_shard(endpoint: &str, ordinal: u32, doc: &str) {
    for _ in 0..600 {
        if let Ok(remote) = RemoteNode::connect(endpoint.to_string(), None).await {
            let node: Arc<dyn Node> = ShardNode::new(Arc::new(remote), HASH_IDX, ordinal).shared();
            let gw = Gateway::new(node);
            if let Ok(ids) = search_ids_idx(&gw, HASH_IDX).await {
                if ids == vec![doc.to_string()] {
                    return;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("{endpoint} never served ordinal {ordinal} read-through");
}

/// Resolve ordinal `ordinal`'s primary endpoint from the CP, retrying until the index is registered
/// (the nodes have announced) and a live holder is placed.
async fn resolve_shard_primary(cp: &mut ControlPlaneClient<Channel>, ordinal: u32) -> String {
    loop {
        match cp
            .resolve_unit_owner(Request::new(ResolveUnitOwnerRequest {
                index: HASH_IDX.into(),
                unit: Some(resolve_unit_owner_request::Unit::Shard(ordinal)),
            }))
            .await
        {
            Ok(r) => return r.into_inner().endpoint,
            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
}

/// D53 hash-shard parity: killing a hash ordinal's PRIMARY node under sustained query fails reads over
/// to the replica with a bounded gap and no `partial` — the ordinal counterpart of the windowed drill.
#[tokio::test]
async fn killing_a_hash_ordinals_primary_fails_reads_over_to_the_replica() {
    // Serialize the port-reservation/process-bind window against this file's other tests.
    let _startup = STARTUP.lock().await;
    let store_dir = tempfile::tempdir().unwrap();

    // 1. Publish TWO ordinals (one doc each) to the shared object store — every drill query then
    //    scatters across both shards, the path where `partial` is reachable.
    seed_ordinal(store_dir.path(), 0, ORD0_DOC).await;
    seed_ordinal(store_dir.path(), 1, ORD1_DOC).await;

    // 2. Two node data dirs with the definition only (they serve the ordinals read-through).
    let a_dir = tempfile::tempdir().unwrap();
    let b_dir = tempfile::tempdir().unwrap();
    define_hash_index(a_dir.path());
    define_hash_index(b_dir.path());

    let addrs = free_addrs(5);
    let (cp_addr, a_addr, b_addr) = (addrs[0].clone(), addrs[1].clone(), addrs[2].clone());
    let (a_health, b_health) = (addrs[3].clone(), addrs[4].clone());

    // 3. Control plane at R=2.
    let cp_dir = tempfile::tempdir().unwrap();
    let _cp = Proc(
        Command::new(env!("CARGO_BIN_EXE_growlerdb"))
            .args([
                "--data-dir",
                cp_dir.path().to_str().unwrap(),
                "control-plane",
                "--addr",
                &cp_addr,
            ])
            .env("GROWLERDB_REPLICATION_FACTOR", "2")
            .stdout(Stdio::null())
            .spawn()
            .expect("spawn control-plane"),
    );
    let cp_ep = format!("http://{cp_addr}");
    // The CP must be serving before the nodes spawn, so their `--register` lands cleanly.
    wait_for_cp_ready(&cp_ep).await;

    // 4. Two pool nodes reading the ordinals through the shared store.
    let a_ep = format!("http://{a_addr}");
    let b_ep = format!("http://{b_addr}");
    let spawn_node = |dir: &std::path::Path, addr: &str, ep: &str, health: &str| {
        Proc(
            Command::new(env!("CARGO_BIN_EXE_growlerdb"))
                .args([
                    "--data-dir",
                    dir.to_str().unwrap(),
                    "--metrics-addr",
                    health,
                    "serve-pool",
                    "--index",
                    HASH_IDX,
                    "--addr",
                    addr,
                    "--register",
                    &cp_ep,
                    "--advertise-addr",
                    ep,
                ])
                .env("GROWLERDB_OBJECT_STORE_FS", store_dir.path())
                .stdout(Stdio::null())
                .spawn()
                .expect("spawn serve-pool"),
        )
    };
    let mut node_a = spawn_node(a_dir.path(), &a_addr, &a_ep, &a_health);
    let mut node_b = spawn_node(b_dir.path(), &b_addr, &b_ep, &b_health);
    wait_for_grpc(&a_ep).await;
    wait_for_grpc(&b_ep).await;
    wait_until_ready(&a_health).await;
    wait_until_ready(&b_health).await;

    // 5. Place both ordinals (R=2 → primary + replica each; with two nodes, both hold both).
    let mut cp = {
        let mut client = None;
        for _ in 0..200 {
            if let Ok(c) = ControlPlaneClient::connect(cp_ep.clone()).await {
                client = Some(c);
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        client.expect("connect control plane")
    };
    let other = |primary: &str| {
        if primary == a_ep {
            b_ep.clone()
        } else {
            a_ep.clone()
        }
    };
    let o0_primary = resolve_shard_primary(&mut cp, 0).await;
    let o1_primary = resolve_shard_primary(&mut cp, 1).await;
    assert!(o0_primary == a_ep || o0_primary == b_ep);
    // Ordinal order (Gateway::sharded indexes nodes by ordinal): [ordinal 0, ordinal 1].
    let units = vec![
        (0u32, o0_primary.clone(), other(&o0_primary)),
        (1u32, o1_primary.clone(), other(&o1_primary)),
    ];

    // 6. Every holder serves both ordinals read-through once the CP push reaches them.
    for ep in [&a_ep, &b_ep] {
        wait_until_serving_shard(ep, 0, ORD0_DOC).await;
        wait_until_serving_shard(ep, 1, ORD1_DOC).await;
    }

    // 7. The drill gateway is built BEFORE the kill and used across it. Sustain queries pre-kill:
    //    both ordinals answer, never partial.
    let gw = sharded_failover_gateway(&units);
    let both = vec![ORD0_DOC.to_string(), ORD1_DOC.to_string()];
    for _ in 0..10 {
        assert_eq!(
            search_ids_idx(&gw, HASH_IDX).await.expect("pre-kill query"),
            both,
            "both ordinals answer before the kill"
        );
    }

    // 8. Kill ordinal 0's PRIMARY node under the ongoing query stream.
    let killed_at = Instant::now();
    if o0_primary == a_ep {
        node_a.kill();
    } else {
        node_b.kill();
    }

    // 9. Sustained queries across the kill on the SAME gateway: a bounded gap, then 40 consecutive
    //    answers covering both ordinals with no partial — the replica absorbed the loss.
    const GRACE: Duration = Duration::from_millis(1000);
    let mut successes = 0u32;
    let mut gap_failures = 0u32;
    for _ in 0..400 {
        match search_ids_idx(&gw, HASH_IDX).await {
            Ok(ids) => {
                assert_eq!(
                    ids, both,
                    "a failover answer must still cover both ordinals"
                );
                successes += 1;
                if successes >= 40 {
                    break;
                }
            }
            Err(e) => {
                assert!(
                    killed_at.elapsed() <= GRACE,
                    "read failed {:?} after the kill (past the {GRACE:?} failover grace, \
                     {successes} successes so far): {e}",
                    killed_at.elapsed()
                );
                successes = 0;
                gap_failures += 1;
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        successes >= 40,
        "no sustained recovery after the primary kill ({gap_failures} failures in the gap)"
    );

    // 10. A FRESH gateway (new channels dialing the dead primary) also answers — cold-dial failover.
    assert_eq!(
        search_ids_idx(&sharded_failover_gateway(&units), HASH_IDX)
            .await
            .expect("fresh-gateway query after the kill"),
        both,
        "a fresh gateway fails over past the dead primary too"
    );
}
