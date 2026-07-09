"""Seed a small sample Iceberg table (`growlerdb.docs`) into the local Polaris catalog.

Creates a few rows (id, title, body) — enough for the M0 walking-skeleton E2E
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

# A time-stamped sensor-readings table for the time-window (task-81) demo/verification: `ingest` is
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
