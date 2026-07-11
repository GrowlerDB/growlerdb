"""Generate a realistic HTTP access-log corpus into Iceberg for the scale test.

Rich rows (~17 fields, ~350-450 B/row). This one module is BOTH corpus entry points:
`load()` bulk-writes for the local Compose smoke / `harness.py load`, and `stream()` is the
in-cluster continuous generator — the generic k8s generator Deployment mounts this file and runs
it, so the row recipe lives in exactly one place. `load` is seeded (`BENCH_SEED`) so runs are
comparable; `client_ip` is drawn from a bounded pool with heavy hitters so cardinality is
realistic (not one-per-row), which is what makes index:source ratios meaningful.

Env (same names as the generator): POLARIS_URI, POLARIS_CATALOG, POLARIS_CREDENTIAL,
AWS_ENDPOINT_URL_S3, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY. BENCH_ROWS = rows at fraction=1.0;
BENCH_BATCH = write batch size; BENCH_SEED = RNG seed.
"""

import os
import random
import time
import uuid

BATCH = int(os.environ.get("BENCH_BATCH", "50000"))
ROWS = int(os.environ.get("BENCH_ROWS", "1000000"))

METHODS = ["GET"] * 6 + ["POST"] * 2 + ["PUT", "DELETE", "HEAD"]
HOSTS = ["api.example.com", "www.example.com", "cdn.example.com", "auth.example.com", "shop.example.com"]
PATHS = ["/api/v1/users", "/api/v1/users/{id}", "/api/v1/orders", "/api/v1/products",
         "/api/v1/search", "/api/v1/cart", "/api/v1/checkout", "/login", "/logout",
         "/static/js/app.bundle.js", "/static/css/main.css", "/images/hero.jpg",
         "/favicon.ico", "/health", "/metrics", "/blog/{slug}", "/", "/pricing", "/docs"]
QUERIES = ["", "", "?page=1", "?page=2&sort=created_at", "?q=running+shoes", "?ref=email",
           "?utm_source=newsletter&utm_medium=email", "?limit=50&offset=100", "?expand=items"]
PROTOCOLS = ["HTTP/1.1"] * 3 + ["HTTP/2.0"] * 5 + ["HTTP/3"]
STATUS = ["200"] * 12 + ["201", "204", "301", "304", "400", "401", "403", "404", "429", "500", "503"]
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148",
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)",
    "curl/8.4.0", "python-requests/2.31.0", "PostmanRuntime/7.37.0",
    "GrowlerDB-HealthCheck/1.0", "Datadog Agent/7.52.0",
]
REFERER_POOL = ["-", "-", "https://www.google.com/", "https://example.com/", "https://example.com/pricing",
            "https://t.co/abc123", "https://news.ycombinator.com/", "android-app://com.example.app"]
REGIONS = ["us-east-1", "us-west-2", "eu-west-1", "eu-central-1", "ap-south-1", "ap-southeast-2", "sa-east-1"]
TAGS = ["prod,web", "prod,api", "prod,mobile", "staging,web", "prod,cdn", "prod,internal"]
BASE_TS = 1700000000


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


def _table_exists(catalog, table):
    try:
        catalog.load_table(table)
        return True
    except Exception:  # noqa: BLE001
        return False


def _schema():
    import pyarrow as pa

    return pa.schema([
        ("request_id", pa.string()), ("ts", pa.int64()), ("method", pa.string()),
        ("host", pa.string()), ("path", pa.string()), ("query", pa.string()),
        ("protocol", pa.string()), ("status", pa.string()), ("response_size", pa.int64()),
        ("response_time_ms", pa.int64()), ("client_ip", pa.string()), ("user_agent", pa.string()),
        ("referer", pa.string()), ("user_id", pa.string()), ("session_id", pa.string()),
        ("region", pa.string()), ("tags", pa.string()),
    ])


def _ip_gen():
    """A bounded client-IP pool with heavy hitters (bots/proxies/NAT) — real logs repeat IPs, so
    cardinality is ~100k, not one-per-row (fully-random IPs maxed the IP index unrealistically)."""
    ip_pool = [f"{random.randint(1,223)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"
               for _ in range(100_000)]
    hot_ips = ip_pool[:500]

    def gen_ip():
        return random.choice(hot_ips) if random.random() < 0.3 else random.choice(ip_pool)

    return gen_ip


def _rows(n, ts_offset, gen_ip):
    """One batch of rich access-log rows — the single row recipe both `load` (bulk) and
    `stream` (continuous k8s generator) draw from."""
    return {
        "request_id": [uuid.uuid4().hex for _ in range(n)],
        "ts": [BASE_TS + ts_offset + i for i in range(n)],
        "method": [random.choice(METHODS) for _ in range(n)],
        "host": [random.choice(HOSTS) for _ in range(n)],
        "path": [random.choice(PATHS) for _ in range(n)],
        "query": [random.choice(QUERIES) for _ in range(n)],
        "protocol": [random.choice(PROTOCOLS) for _ in range(n)],
        "status": [random.choice(STATUS) for _ in range(n)],
        "response_size": [int(random.lognormvariate(7.5, 1.6)) for _ in range(n)],
        "response_time_ms": [int(random.lognormvariate(3.2, 1.0)) for _ in range(n)],
        "client_ip": [gen_ip() for _ in range(n)],
        "user_agent": [random.choice(USER_AGENTS) for _ in range(n)],
        "referer": [random.choice(REFERER_POOL) for _ in range(n)],
        "user_id": [f"user_{random.randint(1,50000):05d}" for _ in range(n)],
        "session_id": [uuid.uuid4().hex[:16] for _ in range(n)],
        "region": [random.choice(REGIONS) for _ in range(n)],
        "tags": [random.choice(TAGS) for _ in range(n)],
    }


def load(table="growlerdb.http_logs", fraction=1.0):
    import pyarrow as pa

    random.seed(int(os.environ.get("BENCH_SEED", "42")))
    gen_ip = _ip_gen()
    schema = _schema()
    catalog = _catalog()
    ns = table.split(".")[0]
    try:
        catalog.create_namespace(ns)
    except Exception:  # noqa: BLE001 — already exists
        pass
    if _table_exists(catalog, table):
        catalog.drop_table(table)
    tbl = catalog.create_table(table, schema=schema)

    total = int(ROWS * fraction)
    written = 0
    while written < total:
        n = min(BATCH, total - written)
        tbl.append(pa.table(_rows(n, written, gen_ip), schema=schema))
        written += n
    return written


def stream(table="growlerdb.http_logs", batch=10, sleep_s=5):
    """Append `batch` rows every `sleep_s` forever — the in-cluster streaming generator:
    the generic k8s generator Deployment mounts this module and calls this.
    Creates the table if absent (never drops — a restart resumes the stream) and prints
    `created <table>` once, the readiness gate deploy/k8s/scale-up.sh waits on."""
    import pyarrow as pa
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField, StringType

    gen_ip = _ip_gen()
    schema = _schema()
    catalog = _catalog()
    catalog.create_namespace_if_not_exists(table.split(".")[0])
    if not _table_exists(catalog, table):
        ice = Schema(*[
            NestedField(i + 1, f.name, LongType() if f.type == pa.int64() else StringType(), required=False)
            for i, f in enumerate(schema)
        ])
        catalog.create_table(table, schema=ice)
        print(f"created {table}", flush=True)
    n = 0
    while True:
        tbl = catalog.load_table(table)  # reload for the latest snapshot
        tbl.append(pa.table(_rows(batch, n, gen_ip), schema=schema))
        n += batch
        print(f"appended {batch} rows to {table} (total ~{n})", flush=True)
        time.sleep(sleep_s)
