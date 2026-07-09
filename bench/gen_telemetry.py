"""Generate synthetic IoT telemetry readings into the Iceberg table `growlerdb.telemetry` for
benchmarking (task-55 quick assessment). Row count via BENCH_ROWS (default 2,000,000), batched by
BENCH_BATCH.

Realistic-ish cardinalities (devices/firmware/gateways/sites), weighted status, templated free-text
`message`, plus a few injected **needles** so needle-in-haystack queries have a real, rare target:
  - gateway = gw-rare-eu7 (~12 rows)
  - message contains "bearing" (~12 rows)
  - status = "critical" via the weighted pool (~1/8 of the 'critical' slot)
"""

import os
import random

import numpy as np
import pyarrow as pa
from pyiceberg.catalog.rest import RestCatalog

ROWS = int(os.environ.get("BENCH_ROWS", "2000000"))
BATCH = int(os.environ.get("BENCH_BATCH", "250000"))
DAYS = 7
BASE_MS = 1_750_000_000_000  # fixed, day-aligned-ish base

catalog = RestCatalog(
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

schema = pa.schema([
    ("id", pa.string()),
    ("ts", pa.int64()),
    ("device_id", pa.string()),
    ("gateway", pa.string()),
    ("site", pa.string()),
    ("firmware", pa.string()),
    ("metric", pa.string()),
    ("subsystem", pa.string()),
    ("status", pa.string()),
    ("reading", pa.int64()),
    ("message", pa.string()),
])

devices = [f"device-{i:03d}" for i in range(200)]
firmwares = [f"fw-2.{i % 20}.{i % 10}" for i in range(2000)]
metrics = ["temperature", "humidity", "pressure", "vibration", "voltage", "current", "rpm", "flow"]
subsystems = ["hvac", "motor", "power", "network", "controller"]
status_pool = ["ok", "ok", "ok", "ok", "info", "warning", "error", "critical"]  # weighted
sites = [f"site-{i:02d}" for i in range(50)]
templates = [
    "{m} reading {v} on {d} via {gw}",
    "{d} reported {m} {v} at {s}",
    "{sub} {m} sample {v} firmware {fw}",
    "gateway {gw} forwarded {m} from {d}",
    "calibration of {m} on {d} at {s}",
]
gateway_pool = [f"gw-{i:03d}" for i in range(200)]

NEEDLE_GATEWAY = "gw-rare-eu7"
NEEDLE_MSG_WORD = "bearing"


def gen_batch(n, offset, inject):
    ids = [f"e{offset + i}" for i in range(n)]
    ts = (BASE_MS + np.random.randint(0, DAYS * 86_400_000, n)).tolist()
    device = random.choices(devices, k=n)
    gw = random.choices(gateway_pool, k=n)
    site = random.choices(sites, k=n)
    fw = random.choices(firmwares, k=n)
    metric = random.choices(metrics, k=n)
    sub = random.choices(subsystems, k=n)
    st = random.choices(status_pool, k=n)
    reading = np.random.randint(0, 1000, n).tolist()
    tix = random.choices(range(len(templates)), k=n)
    msg = [templates[tix[i]].format(m=metric[i], v=reading[i], d=device[i], gw=gw[i],
                                    s=site[i], sub=sub[i], fw=fw[i])
           for i in range(n)]
    if inject:
        for j in range(12):
            gw[j] = NEEDLE_GATEWAY
            st[j] = "critical"
        for j in range(12, 24):
            msg[j] = f"{NEEDLE_MSG_WORD} fault detected on {device[j]} reported by {gw[j]}"
    return pa.table({
        "id": ids, "ts": ts, "device_id": device, "gateway": gw, "site": site, "firmware": fw,
        "metric": metric, "subsystem": sub, "status": st, "reading": reading, "message": msg,
    }, schema=schema)


catalog.create_namespace_if_not_exists("growlerdb")
table = catalog.create_table_if_not_exists("growlerdb.telemetry", schema=schema)

written = 0
first = True
while written < ROWS:
    n = min(BATCH, ROWS - written)
    batch = gen_batch(n, written, inject=first)
    # Overwrite on the first batch (clean append-only history — no delete in history that would
    # trip the connector's changelog read), then append the rest.
    if first:
        table.overwrite(batch)
        first = False
    else:
        table.append(batch)
    written += n
    print(f"  wrote {written:,}/{ROWS:,}", flush=True)

print(f"seeded growlerdb.telemetry with {written:,} readings "
      f"(needles: gateway={NEEDLE_GATEWAY} ~12, message~'{NEEDLE_MSG_WORD}' ~12)")
