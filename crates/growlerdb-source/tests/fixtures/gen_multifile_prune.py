#!/usr/bin/env python3
"""LOCAL Iceberg fixture isolating **cross-file** serial hydration reads: N separate data files, one
scattered target row per file, ts-sorted globally. A broad top-k's OR-of-AND (request_id AND ts) selects
ONE row group in EACH file, so the plan returns N tasks — the shape `scan_stale_index` reads one file at
a time. Companion to gen_rowgroup_prune.py (single-file, row-group level); this one is file level.

Run (any pyiceberg 0.11 + pyarrow venv):
  GDB_MF_WAREHOUSE=/tmp/mfwh python3 gen_multifile_prune.py
Then: cargo test -p growlerdb-source --test multifile_parallel -- --ignored
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

WH = os.environ.get("GDB_MF_WAREHOUSE", "/tmp/mfwh")
N_FILES = 12
ROWS_PER_FILE = 2_000
ROWS_PER_GROUP = 1_000            # -> 2 row groups per file
PAYLOAD_PAD = "x" * 180

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
    # md5 -> uniform, uncorrelated with ts: request_id alone prunes nothing; only the ts sort key does.
    return hashlib.md5(f"req-{i}".encode()).hexdigest()


def main():
    if os.path.exists(WH):
        shutil.rmtree(WH)
    os.makedirs(WH)

    catalog = SqlCatalog("mf", **{"uri": f"sqlite:///{WH}/cat.db", "warehouse": f"file://{WH}"})
    catalog.create_namespace("ns")
    tbl = catalog.create_table("ns.multifile", schema=SCHEMA, sort_order=SORT)

    data_paths = []
    targets = []
    for f in range(N_FILES):
        base = f * ROWS_PER_FILE
        idx = list(range(base, base + ROWS_PER_FILE))
        rids = [rid(i) for i in idx]
        ts = idx                                  # globally increasing -> disjoint per-file ts ranges
        payload = [f"{rid(i)}-{PAYLOAD_PAD}" for i in idx]
        table = pa.table({"request_id": rids, "ts": ts, "payload": payload}, schema=ARROW)
        p = f"{WH}/data-{f:02d}.parquet"
        pq.write_table(table, p, row_group_size=ROWS_PER_GROUP)
        data_paths.append(p)
        # one scattered target per file, in row group 0 (ts = base + 500)
        t = base + 500
        targets.append({"file": f, "i": t, "request_id": rid(t), "ts": t})

    tbl.add_files(data_paths)

    summary = {
        "meta": tbl.metadata_location,
        "n_files": N_FILES,
        "rows_per_file": ROWS_PER_FILE,
        "rows_per_group": ROWS_PER_GROUP,
        "default_sort_order_id": tbl.metadata.default_sort_order_id,
        "targets": targets,
    }
    with open(f"{WH}/summary.json", "w") as fh:
        json.dump(summary, fh, indent=2)
    print(json.dumps(summary, indent=2))
    assert len(data_paths) == N_FILES


if __name__ == "__main__":
    main()
