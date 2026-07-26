//! D53 chaos drill (357.10): **zero-gap read failover**. A parked window lives in a shared
//! local-filesystem object store (`GROWLERDB_OBJECT_STORE_FS` — no MinIO needed). A control plane at
//! `R=2` places two pool nodes as the window's holders (primary + replica); both open it **read-through**
//! from the object store via the CP assignment push. A gateway reads through a `FailoverNode` over
//! `[primary, replica]` — and when the **primary node is killed**, the read fails over to the replica
//! and still answers, with no gap and no `partial`.

use std::collections::BTreeMap;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;

use growlerdb_core::{
    CommitBatch, CompositeKey, Document, IndexDefinition, IndexWriter, LocatedDoc, ResolvedIndex,
    SourceCheckpoint, SourceField, SourceSchema, SourceType, TimeWindowing, Value,
    WindowGranularity,
};
use growlerdb_engine::{FailoverNode, Gateway, Node, RemoteNode, WindowNode};
use growlerdb_index::{LocalIndexStore, ShardId};
use growlerdb_proto::v1::control_plane_client::ControlPlaneClient;
use growlerdb_proto::v1::{resolve_unit_owner_request, ResolveUnitOwnerRequest, SearchRequest};
use tonic::Request;

const IDX: &str = "logs";
const W: i64 = 10;

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
/// serves the window read-through, not from local data.
fn define_index(data_dir: &std::path::Path) {
    std::fs::create_dir_all(data_dir.join(IDX)).unwrap();
    std::fs::write(
        data_dir.join(IDX).join("index.json"),
        serde_json::to_vec(&windowed_index()).unwrap(),
    )
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
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("process did not come up at {endpoint}");
}

/// The `id`s a windowed gateway returns for a `*` search (window unset → the gateway fans to its one
/// window). `None` on a transport error (so the caller can retry during failover).
async fn search_ids(gw: &Gateway) -> Option<Vec<String>> {
    let resp = gw
        .search(Request::new(SearchRequest {
            query: "*".into(),
            limit: 10,
            index: IDX.into(),
            ..Default::default()
        }))
        .await
        .ok()?
        .into_inner();
    // An honest partial (a holder down with no fallback) is a failure for this drill.
    assert!(
        !resp.partial,
        "the gateway must not degrade to partial with a live replica"
    );
    Some(
        resp.hits
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
            .collect(),
    )
}

/// A windowed gateway reading through a `FailoverNode` over `[primary, replica]` — the D53 read path
/// the cluster gateway builds from the CP's holder set. A read tries the primary and, if it's down,
/// fails over to the replica.
async fn failover_gateway(primary_ep: &str, replica_ep: &str) -> Gateway {
    // Lazy connect (as the real gateway does): building a holder over a DOWN endpoint must not fail —
    // it fails fast at query time, which is exactly what lets the FailoverNode fall over to the replica
    // once the primary is killed.
    let holder = |ep: &str| {
        let remote = RemoteNode::connect_lazy(ep.to_string(), None).unwrap();
        WindowNode::new(Arc::new(remote), IDX, W).shared() as Arc<dyn Node>
    };
    let node: Arc<dyn Node> =
        FailoverNode::new(holder(primary_ep), vec![holder(replica_ep)]).shared();
    Gateway::windowed(
        vec![node],
        TimeWindowing::new("ts", WindowGranularity::Daily),
        vec![(W, None, true)],
    )
}

/// Poll `endpoint` (a lone `WindowNode` over `(IDX, W)`) until it serves the parked window's doc —
/// i.e. the node has picked up the CP assignment and opened it read-through.
async fn wait_until_serving(endpoint: &str) {
    for _ in 0..200 {
        if let Ok(remote) = RemoteNode::connect(endpoint.to_string(), None).await {
            let node: Arc<dyn Node> = WindowNode::new(Arc::new(remote), IDX, W).shared();
            let gw = Gateway::windowed(
                vec![node],
                TimeWindowing::new("ts", WindowGranularity::Daily),
                vec![(W, None, true)],
            );
            if let Some(ids) = search_ids(&gw).await {
                if ids == vec!["doc-1".to_string()] {
                    return;
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("{endpoint} never served the read-through window");
}

#[tokio::test]
async fn killing_a_units_primary_fails_reads_over_to_the_replica() {
    // A shared local-fs object store both nodes read the parked window through — no S3/MinIO.
    let store_dir = tempfile::tempdir().unwrap();

    // 1. Park a window (with one doc) into the shared object store, from a throwaway build dir.
    {
        let build = tempfile::tempdir().unwrap();
        let store = LocalIndexStore::open(build.path()).unwrap();
        let resolved = windowed_index();
        let id = ShardId::window(IDX, W);
        let shard = store.create_shard(&id, &resolved).unwrap();
        let key = CompositeKey::new(vec![], vec![("id".into(), Value::from("doc-1"))]);
        let mut f = BTreeMap::new();
        f.insert("id".to_string(), Value::from("doc-1"));
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
        let op = growlerdb_backup::fs_store(store_dir.path()).unwrap();
        growlerdb_backup::cold_park(
            shard,
            IDX,
            W,
            &store.shard_path(&id),
            &build.path().join(".stg"),
            &op,
            &format!("cold/{IDX}/w{W}"),
            Some(serde_json::to_string(&resolved).unwrap()),
        )
        .await
        .unwrap();
    }

    // 2. Two node data dirs with the definition only (they serve the window read-through).
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

    // 4. Two pool nodes: registered into the pool, reading the parked window through the shared store.
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

    // 5. Place the window (R=2 → primary + replica). Retry until the index is registered (the nodes
    //    have announced) and both are live in the pool.
    let mut cp = {
        let mut client = None;
        for _ in 0..200 {
            if let Ok(c) = ControlPlaneClient::connect(cp_ep.clone()).await {
                client = Some(c);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        client.expect("connect control plane")
    };
    let primary_ep = loop {
        match cp
            .resolve_unit_owner(Request::new(ResolveUnitOwnerRequest {
                index: IDX.into(),
                unit: Some(resolve_unit_owner_request::Unit::Window(W)),
            }))
            .await
        {
            Ok(r) => break r.into_inner().endpoint,
            // Index not registered yet / no live node yet — retry.
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(50)).await,
        }
    };
    let replica_ep = if primary_ep == a_ep {
        b_ep.clone()
    } else {
        a_ep.clone()
    };
    assert!(primary_ep == a_ep || primary_ep == b_ep);

    // 6. Both holders serve the window read-through once the CP push reaches them.
    wait_until_serving(&primary_ep).await;
    wait_until_serving(&replica_ep).await;

    // 7. A gateway reading through a FailoverNode over [primary, replica] answers from the primary.
    assert_eq!(
        search_ids(&failover_gateway(&primary_ep, &replica_ep).await)
            .await
            .unwrap(),
        vec!["doc-1"],
        "the two-holder gateway answers before the failover"
    );

    // 8. Kill the PRIMARY node.
    if primary_ep == a_ep {
        node_a.kill();
    } else {
        node_b.kill();
    }

    // 9. Reads fail over to the replica — still the doc, no partial, no gap. (Rebuild the gateway so a
    //    fresh channel dials the now-dead primary and fails fast to the replica; retry briefly while
    //    the primary's socket closes.)
    let gw = failover_gateway(&primary_ep, &replica_ep).await;
    let mut last = None;
    for _ in 0..100 {
        if let Some(ids) = search_ids(&gw).await {
            if ids == vec!["doc-1".to_string()] {
                return; // failover succeeded
            }
            last = Some(ids);
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("read did not fail over to the replica after the primary died (last: {last:?})");
}
