"""Realistic synthetic HTTP access-log corpus for the scale/comparison benchmark.

One module, two entry points: `load()` bulk-writes to Iceberg (local smoke / `harness.py load`),
and `stream()` is the in-cluster generator the k8s generator Deployment mounts and runs — so the row
recipe lives in exactly one place, and parallel generator pods (distinct `BENCH_SEED` per pod) shard
the corpus to reach the target size.

The distributions are deliberately skewed to match real web traffic — uniform-random data would give
flat term dictionaries, no IP-CIDR selectivity, and no partition-pruning signal, none of which reflect
a real search workload. The full model + rationale + parameters are documented in
`bench/scale/synthetic-corpus.md`; keep the two in sync. In brief:

  - Zipf popularity for URL paths, client IPs, and user activity (heavy hitters + long tail).
  - Status/method/size conditioned on path kind (static vs api vs page); ~87% 200 overall.
  - Lognormal response sizes + latencies; 5xx run slower; 304s carry size 0.
  - Diurnal (hour-of-day) + weekly (weekday vs weekend) timestamp density over `SPAN_DAYS`.

Everything is seeded (`BENCH_SEED`) → byte-reproducible. Env: POLARIS_URI, POLARIS_CATALOG,
POLARIS_CREDENTIAL, AWS_ENDPOINT_URL_S3, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY; BENCH_ROWS (rows at
fraction=1.0), BENCH_BATCH (write batch), BENCH_SEED (RNG seed), SPAN_DAYS (timeline span).
"""

import os
import random
import time
from bisect import bisect

try:  # generator telemetry (uncompressed-corpus bytes) — provided by the seed image; absent locally
    import genmetrics as _genmetrics
except ImportError:
    _genmetrics = None

# Parquet bloom filters so a GrowlerDB-vs-Iceberg comparison is FAIR (TASK-343): this table is
# UNPARTITIONED (hash-routed by request_id), so a bloom filter on the equality columns is Iceberg's
# only row-group skip — without it every `request_id=`/`status=`/`trace_id=` predicate full-scans.
# `trace_id` (the searchable X-Request-ID) carries a bloom so the point-lookup pair measures
# GrowlerDB's indexed lookup against Iceberg at its best (bloom-skipped scan), not a full scan.
WRITE_PROPERTIES = {
    "write.parquet.bloom-filter-enabled.column.request_id": "true",
    "write.parquet.bloom-filter-enabled.column.trace_id": "true",
    "write.parquet.bloom-filter-enabled.column.status": "true",
}

BATCH = int(os.environ.get("BENCH_BATCH", "50000"))
ROWS = int(os.environ.get("BENCH_ROWS", "1000000"))
SEED = int(os.environ.get("BENCH_SEED", "42"))
SPAN_DAYS = int(os.environ.get("SPAN_DAYS", "7"))  # 7-day span: at ~50 GB → ~215 req/s avg, ~340 peak
BASE_TS = 1699920000  # 2023-11-14T00:00:00Z — midnight-aligned so hour offsets map to the diurnal curve

# --- value spaces (ordered hot -> cold where Zipf-weighted by rank) --------------------------------

HOSTS = ["www.example.com", "api.example.com", "cdn.example.com", "shop.example.com", "auth.example.com"]
# Paths ordered by popularity — index = Zipf rank. Mix of pages, api endpoints, and static assets.
PATHS = [
    "/", "/index.html", "/search", "/api/v1/products", "/api/v1/users", "/login",
    "/api/v1/cart", "/api/v1/checkout", "/api/v1/orders", "/pricing", "/docs", "/blog",
    "/api/v1/search", "/static/js/app.bundle.js", "/static/css/main.css", "/images/logo.png",
    "/favicon.ico", "/api/v1/products/{id}", "/api/v1/users/{id}", "/static/js/vendor.bundle.js",
    "/images/hero.jpg", "/robots.txt", "/health", "/metrics", "/logout", "/about", "/contact",
    "/static/fonts/roboto.woff2", "/images/product-{id}.jpg", "/sitemap.xml",
]
QUERIES = ["", "", "", "", "?page=1", "?page=2&sort=created_at", "?q=running+shoes",
           "?ref=email", "?utm_source=newsletter&utm_medium=email", "?limit=50&offset=100"]
PROTOCOLS = ["HTTP/1.1", "HTTP/2.0", "HTTP/3"]
PROTOCOL_W = [40, 52, 8]
REGIONS = ["us-east-1", "us-west-2", "eu-west-1", "eu-central-1", "ap-south-1", "ap-southeast-2", "sa-east-1"]
REGION_W = [34, 20, 14, 12, 9, 7, 4]
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0",
    "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)",
    "curl/8.4.0", "python-requests/2.31.0", "PostmanRuntime/7.37.0", "Datadog Agent/7.52.0",
]
UA_W = [34, 20, 15, 10, 6, 4, 3, 2, 2, 2, 1, 1]
REFERER_POOL = ["-", "-", "-", "https://www.google.com/", "https://example.com/",
                "https://example.com/pricing", "https://t.co/abc123",
                "https://news.ycombinator.com/", "android-app://com.example.app"]
REFERER_W = [40, 20, 12, 10, 6, 4, 3, 3, 2]

# Status / method / size / latency conditioned on path kind. Overall ~87% 200.
STATUS = {
    "static": ([200, 304, 404, 500], [80, 15, 4, 1]),
    "api":    ([200, 201, 400, 401, 404, 429, 500, 503], [82, 3, 4, 3, 3, 2, 2, 1]),
    "page":   ([200, 301, 302, 304, 404, 500], [88, 3, 3, 2, 3, 1]),
}
METHOD = {
    "static": (["GET"], [1]),
    "api":    (["GET", "POST", "PUT", "DELETE", "PATCH"], [60, 25, 8, 5, 2]),
    "page":   (["GET", "POST"], [97, 3]),
}
# lognormal (mu, sigma) on bytes; static assets are larger than API JSON.
SIZE_LOGN = {"static": (10.3, 1.0), "api": (7.6, 1.2), "page": (9.6, 0.8)}
# lognormal (mu, sigma) on response_time_ms; api slower than cached static. 5xx add +1.0 to mu.
RT_LOGN = {"static": (2.7, 0.8), "api": (4.1, 1.0), "page": (3.7, 0.9)}

# Diurnal (hour-of-day) and weekly (Mon..Sun) traffic-intensity curves.
HOUR_W = [0.20, 0.15, 0.10, 0.10, 0.12, 0.20, 0.40, 0.60, 0.80, 0.95, 1.00, 1.00,
          0.95, 0.90, 0.85, 0.80, 0.85, 0.90, 1.00, 0.95, 0.85, 0.70, 0.50, 0.30]
WEEKDAY_W = [1.0, 1.0, 1.0, 1.0, 0.95, 0.60, 0.55]
_BASE_WDAY = time.gmtime(BASE_TS).tm_wday  # 0 = Monday

IP_POOL_SIZE = 100_000
USER_POOL_SIZE = 50_000
ZIPF_S_PATH = 1.1
ZIPF_S_IP = 1.1
ZIPF_S_USER = 1.2
_STATIC_EXT = (".css", ".js", ".png", ".jpg", ".ico", ".woff2", ".xml")


def _zipf_cum(n, s):
    """Cumulative Zipf weights for ranks 0..n-1 (weight = 1/(rank+1)**s) — for bisect sampling."""
    cum, acc = [], 0.0
    for k in range(n):
        acc += 1.0 / ((k + 1) ** s)
        cum.append(acc)
    return cum


def _cum(weights):
    cum, acc = [], 0.0
    for w in weights:
        acc += w
        cum.append(acc)
    return cum


def _pick(rng, seq, cum):
    """One weighted draw via bisect on the precomputed cumulative weights (fast inner-loop path)."""
    return seq[bisect(cum, rng.random() * cum[-1])]


def path_kind(path):
    if path.startswith("/api/"):
        return "api"
    if "/static/" in path or "/images/" in path or path.endswith(_STATIC_EXT) or path in ("/robots.txt", "/favicon.ico"):
        return "static"
    return "page"


# Precomputed cumulative weights for the module-static value spaces (path/ip/user pools are per-run).
_PROTO_CUM = _cum(PROTOCOL_W)
_REGION_CUM = _cum(REGION_W)
_UA_CUM = _cum(UA_W)
_REF_CUM = _cum(REFERER_W)
_PATH_CUM = _zipf_cum(len(PATHS), ZIPF_S_PATH)
_QUERY_CUM = _cum([1] * len(QUERIES))
_HOST_CUM = _cum([40, 25, 18, 12, 5])
_HOUR_CUM = _cum(HOUR_W)
_STATUS_CUM = {k: _cum(w) for k, (_, w) in STATUS.items()}
_METHOD_CUM = {k: _cum(w) for k, (_, w) in METHOD.items()}
_PATH_KIND = [path_kind(p) for p in PATHS]  # kind per path index (avoid re-classifying per row)


def _build_model(rng):
    """Per-run pools that depend on the seed: the client-IP pool and the user pool, each with Zipf
    popularity, plus the weekday-weighted day curve over SPAN_DAYS."""
    ip_pool = [f"{rng.randint(1,223)}.{rng.randint(0,255)}.{rng.randint(0,255)}.{rng.randint(1,254)}"
               for _ in range(IP_POOL_SIZE)]
    users = [f"user_{i:05d}" for i in range(USER_POOL_SIZE)]
    day_w = [WEEKDAY_W[(_BASE_WDAY + d) % 7] for d in range(SPAN_DAYS)]
    return {
        "ips": ip_pool, "ip_cum": _zipf_cum(IP_POOL_SIZE, ZIPF_S_IP),
        "users": users, "user_cum": _zipf_cum(USER_POOL_SIZE, ZIPF_S_USER),
        "day_cum": _cum(day_w),
    }


def _rows(n, model, rng):
    """One batch of realistic access-log rows. Path is drawn first (Zipf); status/method/size/latency
    are conditioned on the path kind; timestamps follow the diurnal + weekly curve over SPAN_DAYS."""
    paths_idx = [bisect(_PATH_CUM, rng.random() * _PATH_CUM[-1]) for _ in range(n)]
    request_id, trace_id, ts, method, host, path, query, protocol = [], [], [], [], [], [], [], []
    status, response_size, response_time_ms, client_ip = [], [], [], []
    user_agent, referer, user_id, session_id, region, tags = [], [], [], [], [], []
    for i in range(n):
        pidx = paths_idx[i]
        p = PATHS[pidx]
        kind = _PATH_KIND[pidx]
        st = _pick(rng, STATUS[kind][0], _STATUS_CUM[kind])
        mt = _pick(rng, METHOD[kind][0], _METHOD_CUM[kind])
        sz = 0 if st == 304 else int(rng.lognormvariate(*SIZE_LOGN[kind]))
        rmu, rsg = RT_LOGN[kind]
        rt = int(rng.lognormvariate(rmu + (1.0 if st >= 500 else 0.0), rsg))
        day = _pick(rng, range(SPAN_DAYS), model["day_cum"])
        hour = _pick(rng, range(24), _HOUR_CUM)
        request_id.append(f"{rng.getrandbits(128):032x}")  # seeded, not uuid4 → byte-reproducible
        tid = rng.getrandbits(128)  # searchable X-Request-ID (LB/proxy-injected), UUID-shaped keyword
        trace_id.append(f"{tid >> 96:08x}-{tid >> 80 & 0xffff:04x}-{tid >> 64 & 0xffff:04x}-"
                        f"{tid >> 48 & 0xffff:04x}-{tid & 0xffffffffffff:012x}")
        ts.append(BASE_TS + day * 86400 + hour * 3600 + rng.randrange(3600))
        method.append(mt)
        host.append(_pick(rng, HOSTS, _HOST_CUM))
        path.append(p)
        query.append(_pick(rng, QUERIES, _QUERY_CUM))
        protocol.append(_pick(rng, PROTOCOLS, _PROTO_CUM))
        status.append(str(st))
        response_size.append(sz)
        response_time_ms.append(rt)
        client_ip.append(model["ips"][bisect(model["ip_cum"], rng.random() * model["ip_cum"][-1])])
        user_agent.append(_pick(rng, USER_AGENTS, _UA_CUM))
        referer.append(_pick(rng, REFERER_POOL, _REF_CUM))
        user_id.append(model["users"][bisect(model["user_cum"], rng.random() * model["user_cum"][-1])])
        session_id.append(f"{rng.getrandbits(64):016x}")
        region.append(_pick(rng, REGIONS, _REGION_CUM))
        tags.append("prod,web" if kind != "api" else "prod,api")
    return {
        "request_id": request_id, "trace_id": trace_id, "ts": ts, "method": method, "host": host,
        "path": path, "query": query, "protocol": protocol, "status": status,
        "response_size": response_size, "response_time_ms": response_time_ms, "client_ip": client_ip,
        "user_agent": user_agent, "referer": referer, "user_id": user_id, "session_id": session_id,
        "region": region, "tags": tags,
    }


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
        ("request_id", pa.string()), ("trace_id", pa.string()), ("ts", pa.int64()), ("method", pa.string()),
        ("host", pa.string()), ("path", pa.string()), ("query", pa.string()),
        ("protocol", pa.string()), ("status", pa.string()), ("response_size", pa.int64()),
        ("response_time_ms", pa.int64()), ("client_ip", pa.string()), ("user_agent", pa.string()),
        ("referer", pa.string()), ("user_id", pa.string()), ("session_id", pa.string()),
        ("region", pa.string()), ("tags", pa.string()),
    ])


def load(table="growlerdb.http_logs", fraction=1.0):
    import pyarrow as pa

    rng = random.Random(SEED)
    model = _build_model(rng)
    schema = _schema()
    catalog = _catalog()
    ns = table.split(".")[0]
    try:
        catalog.create_namespace(ns)
    except Exception:  # noqa: BLE001 — already exists
        pass
    if _table_exists(catalog, table):
        catalog.drop_table(table)
    tbl = catalog.create_table(table, schema=schema, properties=WRITE_PROPERTIES)

    total = int(ROWS * fraction)
    written = 0
    while written < total:
        n = min(BATCH, total - written)
        tbl.append(pa.table(_rows(n, model, rng), schema=schema))
        written += n
    return written


def stream(table="growlerdb.http_logs", batch=10, sleep_s=5):
    """Append `batch` rows every `sleep_s` forever — the in-cluster streaming generator (the k8s
    generator Deployment mounts this module and calls it). Creates the table if absent (never drops)
    and prints `created <table>` once, the readiness gate scale-up.sh waits on.

    Restart-safe: the row stream is seeded with per-process entropy, so a container restart appends
    FRESH rows rather than replaying the same seeded `request_id` sequence (which duplicated ~8% of
    keys on the first live 3-generator run and broke count convergence). The model vocabulary stays
    seeded by BENCH_SEED so each pod keeps a stable, disjoint term dictionary across restarts."""
    import pyarrow as pa
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField, StringType

    rng = random.Random(SEED)  # stable per-pod vocabulary (users/paths/IPs) — survives restarts
    model = _build_model(rng)
    # Row stream mixes non-deterministic per-process entropy so a restart does not replay ids/rows.
    row_rng = random.Random(SEED ^ int.from_bytes(os.urandom(8), "big"))
    schema = _schema()
    catalog = _catalog()
    catalog.create_namespace_if_not_exists(table.split(".")[0])
    if not _table_exists(catalog, table):
        ice = Schema(*[
            NestedField(i + 1, f.name, LongType() if f.type == pa.int64() else StringType(), required=False)
            for i, f in enumerate(schema)
        ])
        catalog.create_table(table, schema=ice, properties=WRITE_PROPERTIES)
        print(f"created {table}", flush=True)
    n = 0
    while True:
        cols = _rows(batch, model, row_rng)
        _append_retry(catalog, table, pa.table(cols, schema=schema))
        if _genmetrics is not None:  # report the real uncompressed bytes produced (TASK-342)
            _genmetrics.record_columns(cols, batch)
        n += batch
        print(f"appended {batch} rows to {table} (total ~{n})", flush=True)
        time.sleep(sleep_s)


def _append_retry(catalog, table, arrow_table, attempts=12):
    """Append with optimistic-commit retry. Parallel generator pods (GENERATORS>1) all commit to the
    one Iceberg branch, so pyiceberg raises CommitFailedException when another pod committed first —
    expected contention, not an error. Reload the latest snapshot each attempt and retry with jittered
    exponential backoff so the colliding pods desynchronize instead of crash-looping."""
    from pyiceberg.exceptions import CommitFailedException

    for i in range(attempts):
        tbl = catalog.load_table(table)  # re-read the latest snapshot before each commit attempt
        try:
            tbl.append(arrow_table)
            return
        except CommitFailedException:
            if i == attempts - 1:
                raise
            time.sleep(min(2.0, 0.1 * (2**i)) * (0.5 + random.random()))
