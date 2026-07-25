//! End-to-end test of **gateway → pool node** routing (D52, 357.4): spawn a real `serve-pool` hosting
//! two windowed indexes, then front it with a windowed [`Gateway`] per index built from `WindowNode`s
//! (the distributed routing the cluster gateway uses). A search through each index's gateway must
//! reach that index's shard on the one pool node — proving the gateway routes each `(index, window)`
//! unit to the correct holder even when several indexes share an endpoint.

use std::collections::BTreeMap;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;

use growlerdb_core::{
    CommitBatch, CompositeKey, Document, IndexDefinition, IndexWriter, LocatedDoc, ResolvedIndex,
    SourceCheckpoint, SourceField, SourceSchema, SourceType, TimeWindowing, Value,
    WindowGranularity,
};
use growlerdb_engine::{Gateway, Node, RemoteNode, WindowNode};
use growlerdb_index::{LocalIndexStore, ShardId};
use growlerdb_proto::v1::SearchRequest;
use tonic::Request;

fn windowed_index(index: &str) -> ResolvedIndex {
    let src = SourceSchema::new(
        vec![
            SourceField::new("id", SourceType::String),
            SourceField::new("ts", SourceType::Long),
        ],
        vec![],
        vec!["id".into()],
    );
    IndexDefinition::from_yaml(&format!(
        "name: {index}\nsource: {{ iceberg: {{ catalog: g, table: g.{index} }} }}\nwindowing: {{ field: ts, granularity: daily }}\nmapping: {{ selection: EXPLICIT, fields: [ {{ path: id, type: KEYWORD, fast: true }}, {{ path: ts, format: epoch_ms, fast: true }} ] }}\n",
    ))
    .unwrap()
    .resolve(&src)
    .unwrap()
}

/// Define `index` on disk and build its `w10` shard holding one doc `only`.
fn build_windowed_index(data_dir: &std::path::Path, index: &str, only: &str) {
    let resolved = windowed_index(index);
    std::fs::create_dir_all(data_dir.join(index)).unwrap();
    std::fs::write(
        data_dir.join(index).join("index.json"),
        serde_json::to_vec(&resolved).unwrap(),
    )
    .unwrap();
    let store = LocalIndexStore::open(data_dir).unwrap();
    let shard = store
        .create_shard(&ShardId::window(index, 10), &resolved)
        .unwrap();
    let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(only))]);
    let mut fields = BTreeMap::new();
    fields.insert("id".to_string(), Value::from(only));
    fields.insert("ts".to_string(), Value::from(10_i64));
    IndexWriter::write(
        &shard,
        &CommitBatch::from_upserts(
            vec![LocatedDoc {
                doc: Document::new(key, fields),
                iceberg_file: "f0".into(),
                row_position: 0,
            }],
            SourceCheckpoint::iceberg(1),
            "b1",
        ),
    )
    .unwrap();
}

struct Server(Child);
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// A windowed [`Gateway`] fronting `index`'s window 10 on the pool node at `endpoint` — one
/// `WindowNode` stamping the `(index, 10)` unit, exactly as the cluster gateway builds from the CP.
async fn gateway_for(endpoint: &str, index: &str) -> Gateway {
    let remote = RemoteNode::connect(endpoint.to_string(), None)
        .await
        .expect("connect pool node");
    let node: Arc<dyn Node> = WindowNode::new(Arc::new(remote), index, 10).shared();
    let windowing = TimeWindowing::new("ts", WindowGranularity::Daily);
    Gateway::windowed(vec![node], windowing, vec![(10, None, false)])
}

async fn wait_for_grpc(endpoint: &str) {
    for _ in 0..120 {
        if RemoteNode::connect(endpoint.to_string(), None)
            .await
            .is_ok()
        {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!("serve-pool did not come up at {endpoint}");
}

async fn search_ids(gw: &Gateway, index: &str) -> Vec<String> {
    let resp = gw
        .search(Request::new(SearchRequest {
            query: "*".into(),
            limit: 10,
            index: index.into(),
            ..Default::default()
        }))
        .await
        .expect("gateway search")
        .into_inner();
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
        .collect()
}

#[tokio::test]
async fn gateway_routes_each_unit_to_the_right_index_on_a_pool_node() {
    let tmp = tempfile::tempdir().unwrap();
    build_windowed_index(tmp.path(), "alpha", "alphadoc");
    build_windowed_index(tmp.path(), "beta", "betadoc");

    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let addr = format!("127.0.0.1:{port}");
    let endpoint = format!("http://{addr}");
    let _server = Server(
        Command::new(env!("CARGO_BIN_EXE_growlerdb"))
            .args([
                "--data-dir",
                tmp.path().to_str().unwrap(),
                "serve-pool",
                "--index",
                "alpha",
                "--index",
                "beta",
                "--addr",
                &addr,
            ])
            .stdout(Stdio::null())
            .spawn()
            .expect("spawn growlerdb serve-pool"),
    );
    wait_for_grpc(&endpoint).await;

    // A per-index gateway fronting the SAME pool endpoint routes each unit to its own index's shard,
    // because the WindowNode stamps the (index, window) the pool node dispatches on.
    let alpha_gw = gateway_for(&endpoint, "alpha").await;
    let beta_gw = gateway_for(&endpoint, "beta").await;
    assert_eq!(search_ids(&alpha_gw, "alpha").await, vec!["alphadoc"]);
    assert_eq!(search_ids(&beta_gw, "beta").await, vec!["betadoc"]);
}
