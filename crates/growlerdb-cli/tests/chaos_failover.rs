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
use growlerdb_engine::{FailoverNode, Gateway, Node, RemoteNode, WindowNode};
use growlerdb_index::{LocalIndexStore, ShardId};
use growlerdb_proto::v1::control_plane_client::ControlPlaneClient;
use growlerdb_proto::v1::{resolve_unit_owner_request, ResolveUnitOwnerRequest, SearchRequest};
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

/// The sorted `id`s a windowed gateway returns for a `*` search (window unset → the gateway fans to
/// every window). A transport error *or* an honest `partial` is an `Err` — for this drill a
/// degraded page with a live replica available is exactly as much a failure as no page at all.
async fn search_ids(gw: &Gateway) -> Result<Vec<String>, String> {
    let resp = gw
        .search(Request::new(SearchRequest {
            query: "*".into(),
            limit: 10,
            index: IDX.into(),
            ..Default::default()
        }))
        .await
        .map_err(|s| format!("{:?}: {}", s.code(), s.message()))?
        .into_inner();
    if resp.partial {
        return Err("partial response (a window degraded instead of failing over)".into());
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
/// doc — i.e. the node has picked up the CP assignment and opened it read-through.
async fn wait_until_serving(endpoint: &str, window: i64, doc: &str) {
    for _ in 0..200 {
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

    // Distinct ports for the CP + the two nodes (bound together so they can't collide).
    let addrs = free_addrs(3);
    let (cp_addr, a_addr, b_addr) = (addrs[0].clone(), addrs[1].clone(), addrs[2].clone());

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

    // 4. Two pool nodes: registered into the pool, reading the parked windows through the shared store.
    let a_ep = format!("http://{a_addr}");
    let b_ep = format!("http://{b_addr}");
    let spawn_node = |dir: &std::path::Path, addr: &str, ep: &str| {
        Proc(
            Command::new(env!("CARGO_BIN_EXE_growlerdb"))
                .args([
                    "--data-dir",
                    dir.to_str().unwrap(),
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
    let mut node_a = spawn_node(a_dir.path(), &a_addr, &a_ep);
    let mut node_b = spawn_node(b_dir.path(), &b_addr, &b_ep);
    wait_for_grpc(&a_ep).await;
    wait_for_grpc(&b_ep).await;

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
