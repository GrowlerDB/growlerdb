#!/usr/bin/env python3
"""Generate a LOCAL Iceberg fixture that isolates **row-group-level** hydration pruning.

The sibling `gen_prune_fixture.py` proves *file*-level pruning (6 ts-disjoint files → 1). It does
NOT answer the question the PREDICATE hydration strategy hinges on: within a SINGLE large data file
of many row groups, does iceberg-rust skip the row groups a `ts` predicate can't match — so a
scattered top-k batch (20 hits with `ts` spread across the whole timeline, the real
`topk_hydrated` shape) reads ~one row group per hit instead of the whole file?

This fixture is ONE table, ONE data file, MANY ts-sorted row groups:
  - `ts` is strictly increasing across the file → each row group owns a tight, disjoint ts range
    (per-row-group parquet min/max on ts is selective).
  - `request_id` is a multiplicative hash of the row index → within any contiguous row group it
    spans ~the whole hex space, so a `request_id` predicate can prune NEITHER file NOR row group
    (iceberg-rust 0.10.1 has no bloom-filter support; only column min/max stats prune). This is the
    control: request_id-only reads the whole file.
  - a fat `payload` column makes each row group real bytes, so the Rust test's `bytes_read`
    (iceberg `ScanMetrics`) assertion is meaningful.

Declared sort order [ts, request_id] identity — what Spark `WRITE ORDERED BY ts, request_id`
records — so `sort_field_names` surfaces `ts` as a hint field, mirroring production.

The data file is written with pyarrow directly (exact `row_group_size` in ROWS) then registered
with `add_files`, so the row-group layout is controlled precisely rather than left to the writer's
byte heuristic.

Run (any pyiceberg + pyarrow venv):
  GDB_RG_WAREHOUSE=/tmp/rgwh python3 gen_rowgroup_prune.py
Then: cargo test -p growlerdb-source -- --ignored rowgroup_prune

It prints the row-group count and the scattered target rows (rid/ts) the Rust test asserts on.
"""
import hashlib
import json
import os
import shutil

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.table.sorting import SortField, SortOrder
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import LongType, NestedField, StringType

WH = os.environ.get("GDB_RG_WAREHOUSE", "/tmp/rgwh")
N_ROWS = 40_000
ROWS_PER_GROUP = 1_000            # -> 40 row groups in the one file
PAYLOAD_PAD = "x" * 180           # fat, non-trivial per-row bytes

# Scattered targets: one per widely-separated row group (the broad-topk shape). Row index i is
# also its ts, so these ts land in row groups 2, 11, 20, 29, 38 — spread across the file.
TARGET_ROWS = [2_500, 11_500, 20_500, 29_500, 38_500]

SCHEMA = Schema(
    NestedField(1, "request_id", StringType(), required=False),
    NestedField(2, "ts", LongType(), required=False),
    NestedField(3, "payload", StringType(), required=False),
)
SORT = SortOrder(
    SortField(source_id=2, transform=IdentityTransform()),   # ts
    SortField(source_id=1, transform=IdentityTransform()),   # request_id
)
ARROW = pa.schema([
    ("request_id", pa.string()),
    ("ts", pa.int64()),
    ("payload", pa.string()),
])


def rid(i):
    # md5 -> 128-bit uniform; uncorrelated with ts/row order, so within any row group the
    # request_id values span ~the whole hex space (per-group min/max is unselective — the real
    # hash-routed key, and the control that request_id alone cannot prune file OR row group).
    return hashlib.md5(f"req-{i}".encode()).hexdigest()


def main():
    if os.path.exists(WH):
        shutil.rmtree(WH)
    os.makedirs(WH)

    rids = [rid(i) for i in range(N_ROWS)]
    ts = list(range(N_ROWS))                     # strictly increasing -> tight per-group ranges
    payload = [f"{rid(i)}-{PAYLOAD_PAD}" for i in range(N_ROWS)]
    table = pa.table({"request_id": rids, "ts": ts, "payload": payload}, schema=ARROW)

    data_path = f"{WH}/data.parquet"
    pq.write_table(table, data_path, row_group_size=ROWS_PER_GROUP)
    n_rg = pq.ParquetFile(data_path).num_row_groups

    catalog = SqlCatalog(
        "rg", **{"uri": f"sqlite:///{WH}/cat.db", "warehouse": f"file://{WH}"}
    )
    catalog.create_namespace("ns")
    tbl = catalog.create_table("ns.rowgroups", schema=SCHEMA, sort_order=SORT)
    tbl.add_files([data_path])

    targets = [{"i": i, "request_id": rid(i), "ts": i} for i in TARGET_ROWS]
    summary = {
        "meta": tbl.metadata_location,
        "n_rows": N_ROWS,
        "rows_per_group": ROWS_PER_GROUP,
        "num_row_groups": n_rg,
        "default_sort_order_id": tbl.metadata.default_sort_order_id,
        "targets": targets,
    }
    with open(f"{WH}/summary.json", "w") as f:
        json.dump(summary, f, indent=2)
    print(json.dumps(summary, indent=2))
    assert n_rg >= 20, f"expected many row groups, got {n_rg}"


if __name__ == "__main__":
    main()
