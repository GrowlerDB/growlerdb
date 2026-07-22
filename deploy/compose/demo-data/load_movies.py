"""Load the movie demo corpus into the local lakehouse: parquet → `growlerdb.movies`.

The user-facing half of the opt-in demo corpus (`just demo-data`). Fetches the pre-sliced
parquet — `DEMO_DATA_FILE` (a bind-mounted local path) if set, else `DEMO_DATA_URL` (the
published release asset) — and writes it into Iceberg **first** (the system of record; the
`node-movies` service then builds the search index *from* the table). Idempotent: `overwrite()`
makes a re-run converge to exactly the parquet's rows. `DEMO_DATA_SIZE` caps the row count for a
faster first run — the parquet is decade-interleaved, so a head slice still spans the decades
(embed time at index build scales with it; 0 = all).

Source corpus: Wikipedia movie plots (CC-BY-SA-4.0) — see `build_movies_slice.py`.
Connection env mirrors `seed.py` (in-network defaults set by the compose service).
"""

import os
import urllib.request
import warnings

import pyarrow.parquet as pq
from pyiceberg.catalog.rest import RestCatalog

# Same benign warning seed.py silences: overwrite() on a fresh table matches no rows.
warnings.filterwarnings("ignore", message="Delete operation did not match any records")

local = os.environ.get("DEMO_DATA_FILE", "")
if local:
    path = local
    print(f"loading local parquet {path}")
else:
    url = os.environ["DEMO_DATA_URL"]
    path = "/tmp/movies.parquet"
    print(f"downloading {url}")
    urllib.request.urlretrieve(url, path)

table = pq.read_table(path)
size = int(os.environ.get("DEMO_DATA_SIZE", "0"))
if size > 0:
    table = table.slice(0, size)

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
        "s3.region": os.environ.get("AWS_REGION", "us-east-1"),
        # MinIO can't vend STS creds; write directly with our own S3 creds.
        "header.X-Iceberg-Access-Delegation": "",
    },
)

catalog.create_namespace_if_not_exists("growlerdb")
# Idempotent by drop + recreate + append, NOT create-if-not-exists + overwrite: pyiceberg's
# `overwrite` runs a strict schema-compatibility check that rejects this multi-column table
# ("PyArrow table contains more columns…"), even against a table just created from the same
# schema. Drop-and-recreate converges to exactly the parquet's rows and sidesteps that check.
if catalog.table_exists("growlerdb.movies"):
    catalog.drop_table("growlerdb.movies")
iceberg_table = catalog.create_table("growlerdb.movies", schema=table.schema)
iceberg_table.append(table)
print(f"loaded growlerdb.movies with {table.num_rows} films")
