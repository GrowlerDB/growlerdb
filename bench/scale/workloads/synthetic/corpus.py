"""Generate deterministic, http_logs-shaped rows into Iceberg — the download-free fallback workload.

Seeded (BENCH_SEED) so successive runs are identical and regression-comparable. Row count via
BENCH_ROWS (default 5,000,000), scaled by the harness `--fraction`. Same catalog/S3 env as
bench/gen_telemetry.py and the http_logs loader.
"""

import os
import random

BATCH = int(os.environ.get("BENCH_BATCH", "250000"))
ROWS = int(os.environ.get("BENCH_ROWS", "5000000"))
SEED = int(os.environ.get("BENCH_SEED", "42"))
BASE_TS = 893_964_000  # 1998 World Cup era, matching http_logs

_REQUESTS = [
    "GET /english/images/team_hns.gif HTTP/1.0",
    "GET /english/index.html HTTP/1.0",
    "GET /images/home_fr_button.gif HTTP/1.0",
    "GET /french/images/nav_store.gif HTTP/1.0",
    "POST /cgi-bin/search HTTP/1.0",
]
_STATUS = ["200", "200", "200", "304", "404", "302"]


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


def load(table="growlerdb.synthetic", fraction=1.0):
    import pyarrow as pa

    rng = random.Random(SEED)
    total = int(ROWS * fraction)
    schema = pa.schema([
        ("id", pa.string()), ("ts", pa.int64()), ("clientip", pa.string()),
        ("request", pa.string()), ("status", pa.string()), ("size", pa.int64()),
    ])
    catalog = _catalog()
    ns = table.split(".")[0]
    try:
        catalog.create_namespace(ns)
    except Exception:  # noqa: BLE001
        pass
    try:
        catalog.drop_table(table)
    except Exception:  # noqa: BLE001
        pass
    tbl = catalog.create_table(table, schema=schema)

    written = 0
    while written < total:
        n = min(BATCH, total - written)
        batch = {
            "id": [str(written + i) for i in range(n)],
            "ts": [BASE_TS + rng.randint(0, 86_400 * 30) for _ in range(n)],
            "clientip": [f"211.{rng.randint(0, 255)}.{rng.randint(0, 255)}.{rng.randint(1, 254)}"
                         for _ in range(n)],
            "request": [rng.choice(_REQUESTS) for _ in range(n)],
            "status": [rng.choice(_STATUS) for _ in range(n)],
            "size": [rng.randint(0, 50_000) for _ in range(n)],
        }
        tbl.append(pa.table(batch, schema=schema))
        written += n
    return written
