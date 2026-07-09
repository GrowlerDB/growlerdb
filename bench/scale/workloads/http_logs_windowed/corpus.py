"""The TIME-WINDOWED http_logs corpus (task-159 temporal case) — both entry points (task-214).

`load()` bulk-writes the OSB `http_logs` corpus (point CORPUS_PATH at the downloaded documents
file(s); see the README). `stream()` is the in-cluster continuous generator: it advances a
SYNTHETIC timeline — ~LOGS_PER_DAY events per synthetic day, not wall-clock — so new
day-partitions/windows form continuously as the run proceeds (most parked cold, a few hot).

Either way the table is created **partitioned by an identity `day` column** (= ts // 86400, one
partition per event-day) so GrowlerDB's per-partition machinery engages and the source aligns
with the index's daily windows.
"""

import bz2
import gzip
import json
import os
import random
import time
import uuid
from pathlib import Path

BATCH = int(os.environ.get("BENCH_BATCH", "250000"))
DAY_SECONDS = 86400
BASE_TS = 893964000  # the OSB http_logs corpus era (1998) — windows line up either way


def _catalog():
    from pyiceberg.catalog.rest import RestCatalog

    return RestCatalog(
        "growlerdb",
        uri=os.environ.get("POLARIS_URI", "http://localhost:8181/api/catalog"),
        warehouse=os.environ.get("POLARIS_CATALOG", "growlerdb"),
        credential=os.environ.get("POLARIS_CREDENTIAL", "root:s3cr3t"),
        scope="PRINCIPAL_ROLE:ALL",
        **{
            "s3.endpoint": os.environ.get("AWS_ENDPOINT_URL_S3", "http://localhost:9000"),
            "s3.access-key-id": os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
            "s3.secret-access-key": os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
            "s3.path-style-access": "true",
            "header.X-Iceberg-Access-Delegation": "",
        },
    )


def _open(path):
    if path.suffix == ".bz2":
        return bz2.open(path, "rt")
    if path.suffix == ".gz":
        return gzip.open(path, "rt")
    return open(path)


def _files():
    root = Path(os.environ.get("CORPUS_PATH", "./http_logs_corpus"))
    if root.is_dir():
        return sorted(p for p in root.iterdir() if p.is_file())
    if root.is_file():
        return [root]
    raise SystemExit(
        f"CORPUS_PATH '{root}' not found — download the OSB http_logs corpus first (see README)"
    )


def _table_exists(catalog, table):
    try:
        catalog.load_table(table)
        return True
    except Exception:  # noqa: BLE001
        return False


def _schemas():
    import pyarrow as pa
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.transforms import IdentityTransform
    from pyiceberg.types import LongType, NestedField, StringType

    pa_schema = pa.schema([
        ("id", pa.string()), ("ts", pa.int64()), ("clientip", pa.string()),
        ("request", pa.string()), ("status", pa.string()), ("size", pa.int64()),
        ("day", pa.int64()),
    ])
    ice = Schema(
        NestedField(1, "id", StringType(), required=False),
        NestedField(2, "ts", LongType(), required=False),
        NestedField(3, "clientip", StringType(), required=False),
        NestedField(4, "request", StringType(), required=False),
        NestedField(5, "status", StringType(), required=False),
        NestedField(6, "size", LongType(), required=False),
        NestedField(7, "day", LongType(), required=False),
    )
    # Identity partition on `day` — one Iceberg partition per event-day, aligned with the index's
    # daily windows and readable by GrowlerDB's identity-partition metrics/reconcile.
    spec = PartitionSpec(
        PartitionField(source_id=7, field_id=1000, transform=IdentityTransform(), name="day")
    )
    return pa_schema, ice, spec


def load(table="growlerdb.http_logs_windowed", fraction=1.0):
    import pyarrow as pa

    cols = ("id", "ts", "clientip", "request", "status", "size", "day")
    pa_schema, ice, spec = _schemas()

    catalog = _catalog()
    ns = table.split(".")[0]
    try:
        catalog.create_namespace(ns)
    except Exception:  # noqa: BLE001 — already exists
        pass
    if _table_exists(catalog, table):
        catalog.drop_table(table)
    tbl = catalog.create_table(table, schema=ice, partition_spec=spec)

    written = 0
    batch = {k: [] for k in cols}
    for f in _files():
        with _open(f) as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                d = json.loads(line)
                ts = int(d.get("@timestamp", 0))
                batch["id"].append(str(written))
                batch["ts"].append(ts)
                batch["clientip"].append(d.get("clientip", ""))
                batch["request"].append(d.get("request", ""))
                batch["status"].append(str(d.get("status", "")))
                batch["size"].append(int(d.get("size", 0)))
                batch["day"].append(ts // DAY_SECONDS)
                written += 1
                if len(batch["id"]) >= BATCH:
                    tbl.append(pa.table(batch, schema=pa_schema))
                    batch = {k: [] for k in cols}
    if batch["id"]:
        tbl.append(pa.table(batch, schema=pa_schema))
    return written


def stream(table="growlerdb.http_logs_windowed", batch=10, sleep_s=5):
    """Append http_logs-shaped rows on a SYNTHETIC timeline forever — the in-cluster windowed
    generator (task-214): one synthetic day per LOGS_PER_DAY rows (env, default 750k), jittered
    within the day, so day-partitions/windows form continuously. Creates the partitioned table
    if absent (never drops — a restart resumes) and prints `created <table>` once, the readiness
    gate deploy/k8s/scale-up.sh waits on."""
    import pyarrow as pa

    pa_schema, ice, spec = _schemas()
    logs_per_day = int(os.environ.get("LOGS_PER_DAY", "750000"))
    paths = ["/english/images/team.gif", "/images/home_button.gif", "/cgi-bin/search",
             "/english/index.html", "/french/competition/", "/english/venues/", "/images/logo.gif"]
    methods = ["GET", "GET", "GET", "POST", "HEAD"]
    status = ["200", "200", "200", "304", "404", "302", "500"]
    # Per-container-start random token → ids unique across (re)starts (no duplicate PKs).
    run = os.environ.get("HOSTNAME", "run") + "-" + uuid.uuid4().hex[:6]

    catalog = _catalog()
    catalog.create_namespace_if_not_exists(table.split(".")[0])
    if not _table_exists(catalog, table):
        catalog.create_table(table, schema=ice, partition_spec=spec)
        print(f"created {table} (partitioned by day)", flush=True)
    n = 1000
    while True:
        tbl = catalog.load_table(table)  # reload for the latest snapshot
        # Advance the synthetic timeline: one day per logs_per_day rows, jittered within it.
        day_index = n // logs_per_day
        ts_vals = [BASE_TS + day_index * DAY_SECONDS + random.randint(0, DAY_SECONDS - 1)
                   for _ in range(batch)]
        tbl.append(pa.table({
            "id": [f"req-{run}-{n + i}" for i in range(batch)],
            "ts": ts_vals,
            "clientip": [f"{random.randint(1,223)}.{random.randint(0,255)}."
                         f"{random.randint(0,255)}.{random.randint(1,254)}" for _ in range(batch)],
            "request": [f"{random.choice(methods)} {random.choice(paths)}"
                        f"?id={random.randint(1,1000)} HTTP/1.0" for _ in range(batch)],
            "status": [random.choice(status) for _ in range(batch)],
            "size": [random.randint(0, 50_000) for _ in range(batch)],
            "day": [t // DAY_SECONDS for t in ts_vals],
        }, schema=pa_schema))
        print(f"appended {batch} rows to {table} (synthetic day {day_index})", flush=True)
        n += batch
        time.sleep(sleep_s)
