#!/usr/bin/env python3
"""LOCAL Iceberg fixture isolating **row-group-level** hydration pruning: ONE data file of many ts-sorted
row groups — proves iceberg-rust skips the row groups a `ts` predicate can't match (request_id is the control).

Run (any pyiceberg + pyarrow venv):
  GDB_RG_WAREHOUSE=/tmp/rgwh python3 gen_rowgroup_prune.py
Then: cargo test -p growlerdb-source -- --ignored rowgroup_prune
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
    # md5 -> 128-bit uniform, uncorrelated with ts/row order, so per-group min/max is unselective —
    # the control that request_id alone (hash-routed key) prunes neither file nor row group.
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
