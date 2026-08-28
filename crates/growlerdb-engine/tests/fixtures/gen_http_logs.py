"""Create growlerdb.http_logs through the live Polaris REST catalog (MinIO S3), with
SEVERAL data files (multiple appends) and a `request_id` string identifier key, partition
empty — the http_logs key shape. Mirrors connector #4's backfill source table.

Usage:
  gen_http_logs.py            # drop+create, N appends
  gen_http_logs.py compact    # rewrite: replace ALL data files with new ones (same rows)
"""
import os
import sys
import pyarrow as pa
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.schema import Schema
from pyiceberg.expressions import AlwaysTrue
from pyiceberg.types import LongType, NestedField, StringType

TABLE = "growlerdb.http_logs"
N_FILES = 5
ROWS_PER_FILE = 20

SCHEMA = Schema(
    NestedField(1, "request_id", StringType(), required=False),
    NestedField(2, "trace_id", StringType(), required=False),
    NestedField(3, "ts", LongType(), required=False),
    NestedField(4, "status", StringType(), required=False),
)
ARROW = pa.schema([
    ("request_id", pa.string()),
    ("trace_id", pa.string()),
    ("ts", pa.int64()),
    ("status", pa.string()),
])


def rid(i, j):
    # deterministic 32-hex request_id, unique across files+rows
    return f"{i:016x}{j:016x}"


def file_rows(i):
    base = 1000 + i * 1000
    return pa.table(
        {
            "request_id": [rid(i, j) for j in range(ROWS_PER_FILE)],
            "trace_id": [f"trace-{i}-{j}" for j in range(ROWS_PER_FILE)],
            "ts": [base + j for j in range(ROWS_PER_FILE)],
            "status": ["200"] * ROWS_PER_FILE,
        },
        schema=ARROW,
    )


def all_rows():
    tables = [file_rows(i) for i in range(N_FILES)]
    return pa.concat_tables(tables)


def catalog():
    return RestCatalog(
        "growlerdb",
        uri=os.environ.get("POLARIS_URI", "http://localhost:8181/api/catalog"),
        warehouse=os.environ.get("POLARIS_CATALOG", "growlerdb"),
        credential=os.environ.get("POLARIS_CREDENTIAL", "root:s3cr3t"),
        scope="PRINCIPAL_ROLE:ALL",
        **{
            "s3.endpoint": os.environ.get("AWS_ENDPOINT_URL_S3", "http://localhost:9000"),
            "s3.access-key-id": "minioadmin",
            "s3.secret-access-key": "minioadmin",
            "s3.path-style-access": "true",
            "s3.region": "us-east-1",
            "header.X-Iceberg-Access-Delegation": "",
        },
    )


def dump_state(tbl):
    tbl.refresh()
    snap = tbl.metadata.current_snapshot()
    files = [t.file.file_path for t in tbl.scan().plan_files()]
    print(f"snapshot_id={snap.snapshot_id} data_files={len(files)}")
    for f in sorted(files):
        print("  ", f)


def main():
    cat = catalog()
    cat.create_namespace_if_not_exists("growlerdb")
    if len(sys.argv) > 1 and sys.argv[1] == "compact":
        tbl = cat.load_table(TABLE)
        rows = all_rows()
        # Full overwrite = replace snapshot: delete every current data file, add new ones
        # carrying the same rows. Copy-on-write (no delete files) — matches http_logs CoW.
        tbl.overwrite(rows, overwrite_filter=AlwaysTrue())
        print("== after compaction ==")
        dump_state(tbl)
        return
    try:
        cat.drop_table(TABLE)
    except Exception:
        pass
    tbl = cat.create_table(TABLE, schema=SCHEMA)
    for i in range(N_FILES):
        tbl.append(file_rows(i))
    print("== after appends ==")
    dump_state(tbl)


if __name__ == "__main__":
    main()
