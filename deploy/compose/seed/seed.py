"""Seed a small sample Iceberg table (`growlerdb.docs`) into the local Polaris catalog.

Creates a few rows (id, title, body) — enough for the walking-skeleton E2E
test. Connection details come from environment variables (with localhost defaults
for running on the host; the compose `seed` service overrides them for the
container network).
"""

import os

import pyarrow as pa
from pyiceberg.catalog.rest import RestCatalog

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
        # MinIO can't vend STS creds; write directly with our own S3 creds.
        "header.X-Iceberg-Access-Delegation": "",
    },
)

schema = pa.schema(
    [
        ("id", pa.string()),
        ("title", pa.string()),
        ("body", pa.string()),
    ]
)

rows = pa.table(
    {
        "id": ["doc-1", "doc-2", "doc-3"],
        "title": ["welcome", "iceberg search", "hydration"],
        "body": [
            "hello world, welcome to growlerdb",
            "fast full text search over apache iceberg",
            "search returns keys that hydrate authoritative rows",
        ],
    },
    schema=schema,
)

catalog.create_namespace_if_not_exists("growlerdb")
table = catalog.create_table_if_not_exists("growlerdb.docs", schema=schema)
table.overwrite(rows)  # idempotent: always exactly these rows
print("seeded growlerdb.docs with", rows.num_rows, "rows")

# A time-stamped sensor-readings table for the time-window demo/verification: `ingest` is
# when the reading landed in Iceberg (windows bucket by this), `event` is when the reading was
# actually sampled at the device (queried via the per-window event-time zone-map). r5 is "late" —
# ingested on day 2 but sampled on day 0 — so it lands in the day-2 window yet widens that window's
# event zone-map back to day 0 (the classic late-arriving telemetry case).
DAY = 86_400_000
BASE = (1_750_000_000_000 // DAY) * DAY  # a day-aligned epoch-ms base
readings_schema = pa.schema(
    [
        ("id", pa.string()),
        ("ingest", pa.int64()),  # ingest time (epoch ms) — the window field
        ("event", pa.int64()),  # sample time (epoch ms) — the zone-mapped field
        ("message", pa.string()),
    ]
)
readings = pa.table(
    {
        "id": ["r1", "r2", "r3", "r4", "r5"],
        "ingest": [BASE, BASE, BASE + DAY, BASE + 2 * DAY, BASE + 2 * DAY],
        "event": [BASE, BASE, BASE + DAY, BASE + 2 * DAY, BASE],  # r5 late: ingest day2, sampled day0
        "message": [
            "temperature reading from sensor-a",
            "pressure nominal on pump-2",
            "humidity sample from greenhouse-3",
            "vibration spike on motor-7",
            "late delivered temperature reading",
        ],
    },
    schema=readings_schema,
)
readings_table = catalog.create_table_if_not_exists("growlerdb.readings", schema=readings_schema)
readings_table.overwrite(readings)
print("seeded growlerdb.readings with", readings.num_rows, "rows across 3 ingest days")

# A richer `growlerdb.catalog` table — a catalog of GrowlerDB docs/features, one row per
# concept — that drives the getting-started "query playground". Every field type the mapping demos is
# populated with *varied, predictable* values so each query form (term / phrase / keyword / set /
# numeric+float+date range / CIDR / wildcard / prefix / fuzzy / boost / bool / NOT / match-all /
# regex) returns a small, known result the docs can assert. `published` is epoch-ms (→ DATE via
# `format: epoch_ms`); `server_ip` mixes a 10.0.x.x cohort and a 192.168.x.x cohort for CIDR demos.
def _ms(y, m, d):  # midnight-UTC epoch-ms for a calendar date
    import datetime

    return int(datetime.datetime(y, m, d, tzinfo=datetime.timezone.utc).timestamp() * 1000)


catalog_schema = pa.schema(
    [
        ("id", pa.string()),
        ("title", pa.string()),
        ("body", pa.string()),
        ("category", pa.string()),
        ("author", pa.string()),
        ("views", pa.int64()),
        ("rating", pa.float64()),
        ("published", pa.int64()),  # epoch-ms → DATE via `format: epoch_ms`
        ("server_ip", pa.string()),
        ("archived", pa.bool_()),
    ]
)
catalog_rows = pa.table(
    {
        "id": [
            "cat-01", "cat-02", "cat-03", "cat-04", "cat-05",
            "cat-06", "cat-07", "cat-08", "cat-09", "cat-10",
        ],
        "title": [
            "Getting Started Guide",
            "Search API Reference",
            "Iceberg as System of Record",
            "Windowed Ingestion Tutorial",
            "Multi-Index Gateway Routing",
            "OpenSearch Adapter Reference",
            "Hydration Concept",
            "Shard Routing and Rebalancing",
            "ADR: Canonical Timestamp Micros",
            "Observability and SLIs Guide",
        ],
        "body": [
            "walk from nothing to your first full text search over an apache iceberg table",
            "the rest search endpoint returns ranked document coordinates you hydrate from the lake",
            "iceberg stays the authoritative system of record and growlerdb indexes it for search",
            "bucket streaming telemetry into daily windows and prune queries by event time",
            "one gateway fronts every registered index and routes each request to its named index",
            "point an existing opensearch or elasticsearch client at growlerdb unchanged",
            "search returns keys that hydrate authoritative rows straight from object storage",
            "a partitioned key co locates a partition on a shard for balanced fanout",
            "epoch millis timestamps normalize to canonical micros the one scale every path shares",
            "native sli panels track query rate errors latency hydration and ingestion lag",
        ],
        "category": [
            "guide", "reference", "concept", "tutorial", "reference",
            "reference", "concept", "concept", "adr", "guide",
        ],
        "author": [
            "alice", "bob", "carol", "alice", "dave",
            "bob", "carol", "dave", "carol", "alice",
        ],
        "views": [4800, 3200, 1500, 900, 2100, 650, 1200, 300, 45, 2750],
        "rating": [4.9, 4.6, 4.2, 3.8, 4.4, 3.5, 4.7, 3.1, 4.0, 4.8],
        "published": [
            _ms(2024, 6, 1),   # cat-01
            _ms(2024, 3, 15),  # cat-02
            _ms(2023, 11, 20), # cat-03
            _ms(2024, 1, 10),  # cat-04
            _ms(2024, 5, 5),   # cat-05
            _ms(2023, 8, 30),  # cat-06
            _ms(2023, 12, 12), # cat-07
            _ms(2023, 4, 18),  # cat-08
            _ms(2024, 2, 28),  # cat-09
            _ms(2024, 4, 22),  # cat-10
        ],
        "server_ip": [
            "10.0.0.1", "10.0.1.20", "192.168.1.5", "10.0.2.30", "192.168.1.42",
            "10.0.3.7", "192.168.2.11", "10.0.0.99", "192.168.2.200", "10.0.4.4",
        ],
        "archived": [
            False, False, True, False, False,
            True, False, True, False, False,
        ],
    },
    schema=catalog_schema,
)
catalog_table = catalog.create_table_if_not_exists("growlerdb.catalog", schema=catalog_schema)
catalog_table.overwrite(catalog_rows)
print("seeded growlerdb.catalog with", catalog_rows.num_rows, "rows")
