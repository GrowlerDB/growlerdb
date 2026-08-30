#!/usr/bin/env python3
"""LOCAL Iceberg fixture for the hydration sort-key prune path: ns.sorted ([ts, request_id] identity sort
order) vs ns.unsorted over the same N ts-disjoint files; only the ts hint prunes (request_id spans the
hex space in every file). The Rust test asserts the file-count delta. Env NS_WAREHOUSE (default /tmp/prunewh).
"""
import os
import shutil

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.table.sorting import SortField, SortOrder
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import LongType, NestedField, StringType

WH = os.environ.get("NS_WAREHOUSE", "/tmp/prunewh")
N_FILES = 6
ROWS_PER_FILE = 4  # low span end, target/decoys, high span end
# Target row: present in exactly ONE file (k) with a ts inside only that file's range.
TARGET_FILE = 3
TARGET_RID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

SCHEMA = Schema(
    NestedField(1, "request_id", StringType(), required=False),
    NestedField(2, "trace_id", StringType(), required=False),
    NestedField(3, "ts", LongType(), required=False),
    NestedField(4, "status", StringType(), required=False),
)
# ts, then request_id — identity — as the pipeline's `WRITE ORDERED BY ts, request_id`.
SORT = SortOrder(
    SortField(source_id=3, transform=IdentityTransform()),
    SortField(source_id=1, transform=IdentityTransform()),
)
ARROW = pa.schema([
    ("request_id", pa.string()),
    ("trace_id", pa.string()),
    ("ts", pa.int64()),
    ("status", pa.string()),
])

LO_RID = "00000000000000000000000000000000"
HI_RID = "ffffffffffffffffffffffffffffffff"


def file_rows(i):
    base = 1000 + i * 1000  # file i owns ts in [base, base+900]; ranges are disjoint
    rids = [LO_RID, f"5{i:031x}", f"c{i:031x}", HI_RID]  # span the hex space in every file
    ts = [base, base + 300, base + 600, base + 900]
    if i == TARGET_FILE:
        rids[1] = TARGET_RID
        ts[1] = base + 500  # the target's ts falls only inside file i's range
    return pa.table(
        {
            "request_id": rids,
            "trace_id": [f"trace-{i}-{j}" for j in range(ROWS_PER_FILE)],
            "ts": ts,
            "status": ["200"] * ROWS_PER_FILE,
        },
        schema=ARROW,
    )


def build(catalog, name, sort_order):
    ident = f"ns.{name}"
    kwargs = {"schema": SCHEMA}
    if sort_order is not None:
        kwargs["sort_order"] = sort_order
    tbl = catalog.create_table(ident, **kwargs)
    for i in range(N_FILES):  # one append == one data file == one tight ts range
        tbl.append(file_rows(i))
    meta = tbl.metadata_location
    print(f"{name}: {meta}  sort_order_id={tbl.metadata.default_sort_order_id}")
    return meta


def main():
    if os.path.exists(WH):
        shutil.rmtree(WH)
    os.makedirs(WH)
    catalog = SqlCatalog(
        "prune",
        **{"uri": f"sqlite:///{WH}/cat.db", "warehouse": f"file://{WH}"},
    )
    catalog.create_namespace("ns")
    sorted_meta = build(catalog, "sorted", SORT)
    unsorted_meta = build(catalog, "unsorted", None)
    print("TARGET_FILE", TARGET_FILE, "TARGET_RID", TARGET_RID, "TARGET_TS", 1000 + TARGET_FILE * 1000 + 500)
    print("SORTED_META", sorted_meta)
    print("UNSORTED_META", unsorted_meta)


if __name__ == "__main__":
    main()
