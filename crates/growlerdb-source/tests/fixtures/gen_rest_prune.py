"""Create the prune-repro tables THROUGH the live Polaris REST catalog (not SqlCatalog/StaticTable).

Same shape as tests/fixtures/gen_prune_fixture.py but via RestCatalog + MinIO S3, so the Rust
IcebergReader::connect path (RestCatalog::load_table) is exercised exactly as live. Two tables:
  growlerdb.sorted   — declared sort order [ts identity, request_id identity]
  growlerdb.unsorted — same data + per-file ts clustering, NO declared sort order
"""
import os
import pyarrow as pa
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.schema import Schema
from pyiceberg.table.sorting import SortField, SortOrder
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import LongType, NestedField, StringType

N_FILES = 6
ROWS_PER_FILE = 4
TARGET_FILE = 3
TARGET_RID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

SCHEMA = Schema(
    NestedField(1, "request_id", StringType(), required=False),
    NestedField(2, "trace_id", StringType(), required=False),
    NestedField(3, "ts", LongType(), required=False),
    NestedField(4, "status", StringType(), required=False),
)
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
    base = 1000 + i * 1000
    rids = [LO_RID, f"5{i:031x}", f"c{i:031x}", HI_RID]
    ts = [base, base + 300, base + 600, base + 900]
    if i == TARGET_FILE:
        rids[1] = TARGET_RID
        ts[1] = base + 500
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
    ident = f"growlerdb.{name}"
    try:
        catalog.drop_table(ident)
    except Exception:
        pass
    kwargs = {"schema": SCHEMA}
    if sort_order is not None:
        kwargs["sort_order"] = sort_order
    tbl = catalog.create_table(ident, **kwargs)
    for i in range(N_FILES):
        tbl.append(file_rows(i))
    print(f"{name}: default_sort_order_id={tbl.metadata.default_sort_order_id} "
          f"sort_orders={[ (f.source_id, str(f.transform)) for so in tbl.metadata.sort_orders for f in so.fields ]}")
    return tbl


def main():
    catalog = RestCatalog(
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
    catalog.create_namespace_if_not_exists("growlerdb")
    build(catalog, "sorted", SORT)
    build(catalog, "unsorted", None)
    print("TARGET_FILE", TARGET_FILE, "TARGET_RID", TARGET_RID, "TARGET_TS", 1000 + TARGET_FILE * 1000 + 500)


if __name__ == "__main__":
    main()
