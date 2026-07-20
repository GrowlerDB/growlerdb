"""Load the arXiv demo corpus into the local lakehouse: parquet → `growlerdb.arxiv`.

The user-facing half of the opt-in demo corpus (`just demo-data`). Fetches the pre-sliced
parquet — `DEMO_DATA_FILE` (a bind-mounted local path) if set, else `DEMO_DATA_URL` (the
published release asset) — and writes it into Iceberg **first** (the system of record; the
`node-arxiv` service then builds the search index *from* the table). Idempotent: `overwrite()`
makes a re-run converge to exactly the parquet's rows. `DEMO_DATA_SIZE` caps the row count for
slower machines (0 = all — embed time at index build scales with it).

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
    path = "/tmp/arxiv.parquet"
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
iceberg_table = catalog.create_table_if_not_exists("growlerdb.arxiv", schema=table.schema)
iceberg_table.overwrite(table)
print(f"loaded growlerdb.arxiv with {table.num_rows} papers")
