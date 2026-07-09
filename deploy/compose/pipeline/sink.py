"""Stage 2 of the streaming demo: consume the Kafka/Redpanda `telemetry` topic and **append** the
readings to an Iceberg table (`growlerdb.telemetry_stream`) in micro-batches via pyiceberg. This is
the lake landing zone that GrowlerDB's Spark connector then streams into the index.

Creates the table on first run (append-only schema; identifier `id`). Flushes a batch every
FLUSH_SECS seconds or once BATCH_SIZE rows accumulate, whichever comes first.
"""
import json
import os
import time

import pyarrow as pa
from kafka import KafkaConsumer
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.transforms import IdentityTransform

BROKER = os.environ.get("KAFKA_BROKER", "redpanda:9092")
TOPIC = os.environ.get("TOPIC", "telemetry")
TABLE = os.environ.get("ICEBERG_TABLE", "growlerdb.telemetry_stream")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "500"))
FLUSH_SECS = float(os.environ.get("FLUSH_SECS", "3"))

SCHEMA = pa.schema([
    ("id", pa.string()),
    ("ts", pa.int64()),
    ("device_id", pa.string()),
    ("site", pa.string()),
    ("subsystem", pa.string()),
    ("metric", pa.string()),
    ("status", pa.string()),
    ("reading", pa.int64()),
    ("message", pa.string()),
])


def catalog():
    return RestCatalog(
        "growlerdb",
        uri=os.environ.get("POLARIS_URI", "http://polaris:8181/api/catalog"),
        warehouse=os.environ.get("POLARIS_CATALOG", "growlerdb"),
        credential=os.environ.get("POLARIS_CREDENTIAL", "root:s3cr3t"),
        scope="PRINCIPAL_ROLE:ALL",
        **{
            "s3.endpoint": os.environ.get("AWS_ENDPOINT_URL_S3", "http://minio:9000"),
            "s3.access-key-id": os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
            "s3.secret-access-key": os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
            "s3.path-style-access": "true",
            "header.X-Iceberg-Access-Delegation": "",
        },
    )


def main():
    cat = catalog()
    cat.create_namespace_if_not_exists("growlerdb")
    # format-version 2 so the changelog read the connector uses is well-defined.
    table = cat.create_table_if_not_exists(TABLE, schema=SCHEMA, properties={"format-version": "2"})
    # Partition the lake table by `site` (identity) — demonstrates GrowlerDB's composite key =
    # partition field(s) + identifier: readings co-locate by site, and a point lookup of `id` within
    # a `site` prunes the Iceberg scan to that site's files (fast hydration even after compaction
    # rewrites the locators — task-145). Name-based + idempotent: applied once, to new data.
    if not table.spec().fields:
        with table.update_spec() as update:
            update.add_field("site", IdentityTransform(), "site")
        table = cat.load_table(TABLE)  # reload to pick up the new partition spec
        print(f"sink: partitioned {TABLE} by site", flush=True)
    print(f"sink: {BROKER}/{TOPIC} → Iceberg {TABLE} (batch={BATCH_SIZE}, flush={FLUSH_SECS}s)", flush=True)

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BROKER,
        value_deserializer=lambda b: json.loads(b.decode()),
        auto_offset_reset="latest",
        group_id="growlerdb-sink",
        consumer_timeout_ms=int(FLUSH_SECS * 1000),
    )

    buf = []
    last_flush = time.time()
    total = 0

    def flush():
        nonlocal buf, last_flush, total
        if not buf:
            last_flush = time.time()
            return
        table.append(pa.Table.from_pylist(buf, schema=SCHEMA))
        total += len(buf)
        print(f"  appended {len(buf)} rows ({total} total) → {TABLE}", flush=True)
        buf = []
        last_flush = time.time()

    while True:
        # consumer_timeout_ms breaks the iterator on idle so we still flush on a timer.
        for msg in consumer:
            buf.append(msg.value)
            if len(buf) >= BATCH_SIZE or (time.time() - last_flush) >= FLUSH_SECS:
                flush()
        # idle tick (no messages within the timeout) — flush whatever we have.
        flush()


if __name__ == "__main__":
    main()
