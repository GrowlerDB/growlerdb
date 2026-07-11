//! Cross-process smoke test of the **Python SDK**: seed an index, spawn the
//! real `growlerdb serve` with the REST gateway, and run `clients/python/smoke.py`
//! against it. Skipped (not failed) when `python3` isn't on the PATH, so the suite
//! stays green in environments without Python.

use std::collections::BTreeMap;
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use growlerdb_core::{
    CommitBatch, CompositeKey, Document, IndexDefinition, IndexWriter, LocatedDoc,
    SourceCheckpoint, SourceField, SourceSchema, SourceType, Value,
};
use growlerdb_index::{LocalIndexStore, ShardId};

struct Server(Child);
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Seed `docs` on disk (id/body/city/rank), then drop the store so `serve` can open it.
fn seed(root: &std::path::Path) {
    let src = SourceSchema::new(
        vec![
            SourceField::new("id", SourceType::String),
            SourceField::new("body", SourceType::String),
            SourceField::new("city", SourceType::String),
            SourceField::new("rank", SourceType::Long),
        ],
        vec![],
        vec!["id".into()],
    );
    let resolved = IndexDefinition::from_yaml(
        "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: body, type: TEXT }, { path: city, type: KEYWORD }, { path: rank, type: LONG, fast: true } ] }\n",
    )
    .unwrap()
    .resolve(&src)
    .unwrap();
    std::fs::create_dir_all(root.join("docs")).unwrap();
    std::fs::write(
        root.join("docs/index.json"),
        serde_json::to_vec(&resolved).unwrap(),
    )
    .unwrap();

    let store = LocalIndexStore::open(root).unwrap();
    let shard = store
        .create_shard(&ShardId::single("docs"), &resolved)
        .unwrap();
    let doc = |id: &str, body: &str, city: &str, rank: i64| {
        let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]);
        let mut f = BTreeMap::new();
        f.insert("id".to_string(), Value::from(id));
        f.insert("body".to_string(), Value::from(body));
        f.insert("city".to_string(), Value::from(city));
        f.insert("rank".to_string(), Value::Int(rank));
        LocatedDoc {
            doc: Document::new(key, f),
            iceberg_file: "data/f0.parquet".into(),
            row_position: 0,
        }
    };
    IndexWriter::write(
        &shard,
        &CommitBatch::from_upserts(
            vec![
                doc("doc-1", "iceberg search engine", "berlin", 30),
                doc("doc-2", "iceberg lakehouse", "bern", 10),
            ],
            SourceCheckpoint::iceberg(5),
            "b1",
        ),
    )
    .unwrap();
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[test]
fn python_sdk_drives_search_suggest_and_admin_against_a_live_server() {
    // Skip cleanly when there is no Python interpreter available.
    if Command::new("python3").arg("--version").output().is_err() {
        eprintln!("skipping python_sdk smoke: python3 not found");
        return;
    }

    let tmp = tempfile::tempdir().unwrap();
    seed(tmp.path());

    let grpc = format!("127.0.0.1:{}", free_port());
    let rest = format!("127.0.0.1:{}", free_port());
    let _server = Server(
        Command::new(env!("CARGO_BIN_EXE_growlerdb"))
            .args([
                "--data-dir",
                tmp.path().to_str().unwrap(),
                "serve",
                "docs",
                "--addr",
                &grpc,
                "--rest-addr",
                &rest,
            ])
            .stdout(Stdio::null())
            .spawn()
            .expect("spawn growlerdb serve"),
    );

    // Wait for the REST listener to accept connections.
    let mut up = false;
    for _ in 0..100 {
        if TcpStream::connect(&rest).is_ok() {
            up = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    assert!(up, "REST gateway did not come up at {rest}");

    let smoke = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../clients/python")
        .canonicalize()
        .unwrap();
    let output = Command::new("python3")
        .arg(smoke.join("smoke.py"))
        .arg(format!("http://{rest}"))
        .env("PYTHONPATH", &smoke)
        .output()
        .expect("run python smoke");

    assert!(
        output.status.success(),
        "python smoke failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}
